---
layout: post
title: "流计算基础（四）：循环中的增量计算"
date: 2026-11-09 10:00:00 +0800
categories: stream-processing
tags: [differential-dataflow, 增量计算, 递归, distinct]
---

<style>
.post-content { font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Helvetica Neue", "Segoe UI", "Microsoft YaHei", sans-serif; font-size: 1.0625rem; line-height: 1.85; }
.post-content h2 { font-size: 1.5rem; font-weight: 700; margin-top: 2.4em; }
.post-content h3 { font-size: 1.2rem; font-weight: 600; margin-top: 1.8em; }
.post-content table { width: 100%; border-collapse: collapse; font-size: .9375rem; line-height: 1.6; }
.post-content th { background: #f6f1e7; padding: 10px 12px; text-align: left; }
.post-content td { padding: 10px 12px; border-bottom: 1px solid #e8e0d4; }
.post-content .ic-figure { margin: 28px 0; padding: 18px; background: #fffdf8; border: 1px solid #e8e0d4; border-radius: 12px; }
.post-content .ic-scroll { overflow-x: auto; }
.post-content .ic-figure svg { display: block; width: 100%; min-width: 620px; height: auto; }
.post-content .ic-figure figcaption { margin-top: 12px; color: #57534e; font-size: .875rem; }
.post-content .ic-formula { padding: 16px 20px; border-left: 4px solid #0f766e; background: #f6f1e7; margin: 24px 0; }
.post-content .ic-formula p { margin: 0; }
@media (max-width: 600px) {
  .post-title { font-size: 1.85rem; }
  .post-content table { display: block; overflow-x: auto; }
  .post-content .ic-figure { padding: 12px; }
}
</style>

我的 OceanBase 同事李金懋做了一套 DBSP 教程《从变化流到增量运行时》。第一课从一个数据库问题讲起。基表删了一行，已经存好的查询结果要改哪些行？把查询重跑一遍，再和旧结果比较，就能知道答案。DBSP 给出了一套规则，让查询利用已有的状态，从基表的变化直接算出结果的变化。[阅读教程]({{ '/dbsp-ivm/' | relative_url }})

第 7 课讲去重。假设甲的名字出现了两次，删掉其中一次，名单里仍然有甲。到了第 11 课，查询多了一个循环，上一轮找到的名字又成了下一轮的输入。对我来说，这一步只看公式还是抽象。我想知道，一条记录被删掉以后，程序到底要改哪一轮的结果？

为看清这件事，我把第二篇的持股查询缩到一个人、两家公司，用 Differential Dataflow 跑了一遍。我们可以先在纸上算，再看程序实际发出了什么。

## 1. 一次删除怎样进入循环 {#changes}

### 1.1 卖掉直接持股以后，甲还在名单里吗？

P 直接持有甲、乙两家公司的股份。甲和乙也互相持股。下面每个箭头就是持股表里的一行。

```text
P → 甲      P → 乙
甲 → 乙     乙 → 甲
```

我们要列出从 P 出发，沿持股关系能找到的所有公司。每家公司只列一次。起初名单上有甲和乙。现在 P 卖掉了直接持有的全部甲公司股份，持股表删去 `P→甲`。你会从名单里划掉谁？

顺着剩下的边走，P 仍持有乙，乙仍持有甲。名单一行也不用改。可我在循环里的去重算子 `distinct` 后面记了一份日志，看到它先撤回一份甲，又补回一份甲。**最终名单没有变化，这两条更新各自改了什么？**

### 1.2 在哪里看到了这两条更新

实验给初始持股表编号为版本 0，给删去 `P→甲` 后的表编号为版本 1。初次计算时，只有 P 在循环入口。JOIN 沿持股关系走一步就找到甲和乙。去重算子 distinct 各保留一份。反馈边把结果送回入口，下一轮便可沿甲、乙的持股关系继续查找。

<figure class="ic-figure">
<div class="ic-scroll">
<svg viewBox="0 0 700 310" role="img" aria-labelledby="ic-flow-title ic-flow-desc">
  <title id="ic-flow-title">可达性计算的反馈循环</title>
  <desc id="ic-flow-desc">循环入口经过 JOIN 边表、合并起点 P 和 distinct。distinct 的输出经反馈把轮次加一，也可以离开循环，去掉轮次。</desc>
  <defs><marker id="ic-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#57534e"/></marker></defs>
  <g fill="none" stroke="#57534e" stroke-width="2" marker-end="url(#ic-arrow)">
    <path d="M145 145 H205"/><path d="M305 145 H365"/><path d="M490 145 H545"/>
    <path d="M255 69 V120"/><path d="M427 69 V120"/>
    <path d="M575 170 V225 H82 V170"/><path d="M635 170 V255"/>
  </g>
  <g fill="#fff" stroke="#d6d3d1" stroke-width="1.5">
    <rect x="20" y="120" width="125" height="50" rx="8"/>
    <rect x="205" y="120" width="100" height="50" rx="8"/>
    <rect x="365" y="120" width="125" height="50" rx="8"/>
    <rect x="205" y="25" width="100" height="44" rx="8"/>
    <rect x="365" y="25" width="125" height="44" rx="8"/>
    <rect x="535" y="255" width="155" height="44" rx="8"/>
  </g>
  <rect x="545" y="120" width="120" height="50" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/>
  <g fill="#1c1917" font-size="17" text-anchor="middle" font-family="inherit">
    <text x="82" y="151">循环入口</text><text x="255" y="151">JOIN</text>
    <text x="427" y="151">合并起点 P</text><text x="605" y="151">distinct</text>
    <text x="255" y="53">边表</text><text x="427" y="53">起点 P</text>
    <text x="325" y="212">反馈：轮次 +1</text>
    <text x="612" y="283" font-size="15">外层输出：去掉轮次</text>
  </g>
</svg>
</div>
<figcaption>下文主要观察 distinct 的输入和输出。数据经过反馈后，轮次才加 1。</figcaption>
</figure>

时间戳写成 `(输入版本, 轮次)`。更新写成 `记录 @时间 权重变化`。

在 distinct 的输出端，刚才看到的两条日志可以写成下面这样。

```text
甲 @(1,0) -1    版本 1，第 0 轮撤回一份甲
甲 @(1,1) +1    版本 1，第 1 轮补回一份甲
```

同一次删除，两条更新的版本号相同，轮次相差 1。要弄清这个差别，先看删除之前，甲在哪两轮出现过。

## 2. distinct 保留哪些历史 {#history}

先看版本 0。第 0 轮，JOIN 沿 `P→甲` 找到甲。第 1 轮，乙从反馈边回到循环入口，JOIN 沿 `乙→甲` 又找到甲。下面两列都记更新，每行列出这一轮在 distinct 两侧能看到的变化。

| distinct 的输入历史 | distinct 的输出历史 |
|---|---|
| 甲 `@(0,0) +1` | 甲 `@(0,0) +1` |
| 甲 `@(0,1) +1` | 这次没有新输出 |

第 1 轮，甲的输入累计到了两份。distinct 之前已经输出过一份甲，所以这一轮没有新输出。等到下一版本删除 `P→甲`，算子还要查这两列。左列告诉它甲还剩几份支持，右列告诉它下游已经收到哪些更新。

| 状态 | 保存什么 | 用来做什么 |
|---|---|---|
| 输入历史 | 记录、时间、权重变化 | 重建某个时间的输入累计权重 |
| 输出历史 | 已产生的输出更新 | 重建某个时间的已有输出 |
| 待处理任务 | 需要继续计算的记录和时间 | 在后续激活时继续修正 |
| capability | 尚需输出的时间权限 | 让待处理任务能够合法发送更新 |

## 3. 修正怎样算出来 {#correction}

### 3.1 先确定哪些历史属于这个时间

本例按两个坐标分别比较时间：

```text
(a,b) ≤ (c,d)，要求 a≤c 且 b≤d。
```

计算时间 t 的结果时，算子只累计时间 `s≤t` 的历史。

因此，`(0,1)` 不能计入 `(1,0)`。虽然输入版本 0 较早，但第 1 轮不属于第 0 轮。两个时间不可比。

**物理上已经收到，不代表计算这个逻辑时间时可以计入。** 输入历史和输出历史都要遵守这个条件。

### 3.2 用目标输出减去已有输出

固定记录甲，再固定一个时间 t。distinct 先累计符合条件的输入。累计权重非零时，目标输出是 1。累计权重为零时，目标输出是 0。[distinct 实现](https://github.com/TimelyDataflow/differential-dataflow/blob/e6c5a4d92d0bc15bc1b4b9f801618962cd4742a7/differential-dataflow/src/collection.rs#L850)

接着，distinct 累计符合条件的输出历史。这个累计值就是已有输出。最后，算子计算两者的差额：

<div class="ic-formula"><p><strong>本次修正 = 目标输出 − 已有输出。</strong><br>两边都针对同一条记录、同一个逻辑时间。</p></div>

差额为零时，算子不发送数据更新。差额非零时，算子发送修正。它还会把修正加入输出历史。

现在删除 `P→甲`。JOIN 在 `(1,0)` 撤回一份甲。把这次撤回加到版本 0 的两条更新里，就得到了甲在 distinct 的输入历史。

```text
甲 @(0,0) +1
甲 @(0,1) +1
甲 @(1,0) -1
```

计算 `(1,0)` 时，第二条不能计入。输入累计权重是 `1−1=0`。目标输出是 0，已有输出却是 1。distinct 因此发送 `甲 @(1,0) -1`。

再计算 `(1,1)`。三条输入都能计入。输入累计权重是 `1+1−1=1`，目标输出是 1。

此时，输出历史已经包含刚才的撤回：

```text
甲 @(0,0) +1
甲 @(1,0) -1
```

它们在 `(1,1)` 累计为 0。目标输出是 1，已有输出是 0。distinct 因此发送 `甲 @(1,1) +1`。

| 计算时间 | 这一轮能用的路径 | 目标输出 | 修正前的已有输出 | 本次修正 |
|---|---|---:|---:|---:|
| `(1,0)` | 直达边已删除 | 0 | 1 | −1 |
| `(1,1)` | 经乙仍可到达甲 | 1 | 0 | +1 |

把版本 0 和版本 1 的第 1 轮放在一起看，甲的支持数从 2 降到了 1。两个版本都应该保留甲。但在版本 1，第 0 轮的撤回也计入第 1 轮。distinct 要再发一条 `+1`，下游在 `(1,1)` 累计才能得到一份甲。

可以在下面的演示中来回切换第 3、4 步。两步使用同一份输入历史。留意 `(0,1) +1` 何时开始计入，再看输出历史里多了哪一条撤回。

{% include recursive-correction-demo.html %}

### 3.3 谁触发了甲在第 1 轮的修正

查看 JOIN 输出，版本 1 给甲的唯一一条更新是 `(1,0) −1`。那么，谁让 distinct 又检查了一次 `(1,1)`？

distinct 的底层算子 reduce 收到撤回后，按键找到甲的旧支持 `(0,1) +1`。要同时计入旧支持和新撤回，版本号至少是 1，轮次也至少是 1。这两个时间的最小上界给出了最早需要合起来看的时间。

```text
最小上界((1,0), (0,1)) = (1,1)
```

reduce 把这样的时间列入候选。进度允许时，它检查甲在该时间的输入与输出历史，算出那条 `+1`。[候选时间的生成](https://github.com/TimelyDataflow/differential-dataflow/blob/e6c5a4d92d0bc15bc1b4b9f801618962cd4742a7/differential-dataflow/src/operators/reduce.rs#L673)

### 3.4 代码怎样找到甲，再发出修正

distinct 在输入侧按记录建立索引，甲本身就是一个键。Differential Dataflow 把新索引批次的数据流和保存历史批次的 trace 合称为 arrangement。reduce 另有一份输出 trace。处理甲时，reduce 从两侧读取这个键的历史，并在临时缓冲区里按时间累计。[arrangement 定义](https://github.com/TimelyDataflow/differential-dataflow/blob/e6c5a4d92d0bc15bc1b4b9f801618962cd4742a7/differential-dataflow/src/operators/arrange/arrangement.rs#L45) · [reduce 读取历史](https://github.com/TimelyDataflow/differential-dataflow/blob/e6c5a4d92d0bc15bc1b4b9f801618962cd4742a7/differential-dataflow/src/operators/reduce.rs#L604)

下面是 `reduce_abelian` 对当前键、当前时间求差额的代码。

```rust
if !input.is_empty() {
    logic(key, input, change);
}
change.extend(output.drain(..).map(|(x, mut d)| {
    d.negate();
    (x, d)
}));
crate::consolidation::consolidate(change);
```

`logic` 计算目标输出。随后，代码加入已有输出的负值。`consolidate` 合并同一记录的权重。这些操作发生在当前键、当前待计算时间内。[完整实现](https://github.com/TimelyDataflow/differential-dataflow/blob/e6c5a4d92d0bc15bc1b4b9f801618962cd4742a7/differential-dataflow/src/operators/arrange/arrangement.rs#L280)

内部出现正值和负值，并不表示它们已经分别发往下游。合计为零时，没有数据更新。

版本 1 中的两条甲更新发生在不同时间。保留轮次时，它们不能直接抵消。否则，系统会丢失“第 0 轮没有甲”的信息。

## 4. 修正怎样继续传播 {#propagation}

### 4.1 甲的修正影响乙的支持数

甲的两条输出经过反馈。轮次分别加 1。JOIN 再沿 `甲→乙` 传播变化。

| distinct 输出 | 反馈后的循环入口 | JOIN 输出 |
|---|---|---|
| 甲 `@(1,0) -1` | 甲 `@(1,1) -1` | 乙 `@(1,1) -1` |
| 甲 `@(1,1) +1` | 甲 `@(1,2) +1` | 乙 `@(1,2) +1` |

所以，日志中的 `乙 @(1,2) +1` 来自 JOIN。它补回乙从甲得到的一份支持。

乙的 distinct 输入历史如下：

```text
乙 @(0,0) +1    P→乙
乙 @(0,1) +1    甲→乙
乙 @(1,1) -1    甲的撤回沿边传播
乙 @(1,2) +1    甲的正更新沿边传播
```

| 计算时间 | 乙的输入累计权重 | 目标输出 | 已有输出 | 本次修正 |
|---|---:|---:|---:|---:|
| `(1,0)` | 1 | 1 | 1 | 0 |
| `(1,1)` | 1 | 1 | 1 | 0 |
| `(1,2)` | 2 | 1 | 1 | 0 |

乙一直保留一份输出。因此，版本 1 的 distinct 没有发送乙的变化。

这里的支持数 2 表示两份直接 JOIN 输入。一份来自 `P→乙`，另一份来自 `甲→乙`。它不表示两条互相独立的起点路径。甲本身也可以经由乙到达。

### 4.2 再删除最后一条起点边

等版本 1 的结果完整后，我们再删去 `P→乙`，得到版本 2。现在 P 无法到达甲、乙。只看稳定后的支持总数，很容易误以为两者还能互相支持。

带轮次的历史保留了这些支持的产生过程。删除从最早受影响的轮次开始传播：

| 步骤 | 位置 | 更新 |
|---|---|---|
| 1 | JOIN 输出 | 乙 `@(2,0) -1` |
| 2 | distinct 输出 | 乙 `@(2,0) -1` |
| 3 | 反馈后的循环入口 | 乙 `@(2,1) -1` |
| 4 | JOIN 输出 | 甲 `@(2,1) -1` |
| 5 | distinct 输出 | 甲 `@(2,1) -1` |
| 6 | 反馈后的循环入口 | 甲 `@(2,2) -1` |
| 7 | JOIN 输出 | 乙 `@(2,2) -1` |

最后一条更新撤回乙来自甲的较晚支持。此时乙在 `(2,2)` 的输入累计权重变为 0。已有输出也已经是 0。因此，distinct 不会再撤回一次乙。

这一版本完成后，外层答案为空。甲、乙之间的环没有保住任何从 P 出发的可达结果。

## 5. 修正何时完成 {#completion}

### 5.1 循环内的变化与外层答案

离开这个循环时，系统去掉时间中的轮次。版本 1 的两条甲更新都变成外层时间 1 的更新：

```text
循环内                      外层
甲 @(1,0) -1       →         甲 @1 -1
甲 @(1,1) +1       →         甲 @1 +1
```

它们在外层的净变化为零。版本 1 的最终答案仍是甲、乙。

这次运行中，外层先收到甲的撤回，随后收到甲的增加。读者如果只看第一条消息，会以为甲已经不可达。实验在外层结果流上放了一个 probe。等这个端口的 frontier 中所有时间都大于 1，程序才读取版本 1 的完整答案。此时两条更新已经抵消，甲仍在答案中。

### 5.2 结果完整以后，历史仍可能有用

旧版本完成后，新输入仍可能需要旧状态。例如，新边到达时，JOIN 还要查找此前已经可达的节点。

trace 因此不能仅凭一个版本完成就清空历史。它可以在读取需求允许时压缩历史。压缩必须保留后续读取所需的累计结果。

原始日志也不必永久逐条保存。压缩可以改变旧更新的时间表示。只要读者仍需要区分两个时间，系统就必须保留这份区别。

### 5.3 再嵌套一层循环

本例只有一个反馈循环。两个时间坐标分别表示输入版本和循环轮次。

再嵌套一层循环，可以增加一个内层轮次坐标。对应的时间形如 `(输入版本, 外层轮次, 内层轮次)`。算子仍按偏序选择历史，再计算目标输出与已有输出的差额。

增加的时间坐标保留了更多计算阶段的区别。每一条修正都带着自己的逻辑时间，参与后续计算。

## 代码与延伸阅读

上面的两次删除来自同一个单 worker 实验。实验分别记录循环入口、JOIN 输出、distinct 输出和外层输出。原始日志使用 `0=P`、`1=甲`、`2=乙`。

- [实验源码]({{ '/assets/examples/recursive-delete/recursive_delete_quiz.rs' | relative_url }})与[完整运行日志]({{ '/assets/examples/recursive-delete/verified-run.log' | relative_url }})。`RAW` 编号表示这次运行的观察先后；后面的汇总表按记录和时间排序。
- 实验版本：Differential Dataflow `0.25.1`（提交 `e6c5a4d`），Timely Dataflow `0.31.0`（提交 `eba4ae52`）。文中的源码链接固定到这些版本。
- [Differential Dataflow 论文](https://www.microsoft.com/en-us/research/wp-content/uploads/2013/01/differentialdataflow.pdf)：偏序时间上的差分，以及算子如何利用历史计算变化。
- [官方教程：Reduce、Distinct 与 Threshold](https://timelydataflow.github.io/differential-dataflow/chapter_2/chapter_2_6.html)：多重集、集合语义和按键归约。
- [Shared Arrangements](https://www.vldb.org/pvldb/vol13/p1793-mcsherry.pdf)：多个计算怎样共享索引和历史。

> 系列阅读：[一：并行与 DAG]({{ '/2026/07/27/stream-lecture-1-dag-and-loop/' | relative_url }}) · [二：循环与无界计算]({{ '/2026/10/28/stream-lecture-2-loops-and-unbounded-computation/' | relative_url }}) · [三：事件时间、状态与容错]({{ '/2026/08/05/stream-lecture-2-time-state-and-fault-tolerance/' | relative_url }}) · [四：循环中的增量计算]({{ '/2026/11/09/stream-lecture-4-incremental-computation-in-loops/' | relative_url }})
