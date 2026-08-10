---
layout: post
title: "流计算基础（二）：从有界流到无界流，从理想机器到现实机器"
date: 2026-08-05 10:00:00 +0800
categories: stream-processing
tags: [storm, flink, timely-dataflow, watermark, state, checkpoint]
---

<style>
.post-content {
  font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Helvetica Neue", "Segoe UI", "Microsoft YaHei", sans-serif;
  font-size: 1.0625rem;
  line-height: 1.85;
}
.post-content h2 { font-size: 1.5rem; font-weight: 700; letter-spacing: -0.01em; margin-top: 2.4em; }
.post-content h3 { font-size: 1.2rem; font-weight: 600; margin-top: 1.8em; }
.post-content table { border-collapse: collapse; table-layout: auto; width: 100%; font-size: 0.9375rem; line-height: 1.6; }
.post-content th { background: #f6f1e7; font-weight: 600; padding: 10px 12px; text-align: left; border-bottom: 1px solid #e8e0d4; }
.post-content td { padding: 10px 12px; border-bottom: 1px solid #efe8db; vertical-align: top; }
.fig-card { background: #fffdf8; border: 1px solid #e8e0d4; border-radius: 14px; padding: 24px; margin: 32px 0; }
.fig-svg { width: 100%; height: auto; display: block; font-family: inherit; }
.fig-caption { margin-top: 12px; font-size: 0.85rem; font-weight: 500; color: #57534e; letter-spacing: 0.01em; }
.callout { padding: 16px 24px; border-radius: 0 10px 10px 0; background: #f6f1e7; margin: 24px 0; }
.callout p { margin: 0; }
.callout--insight { border-left: 4px solid #0f766e; }
.callout--caution { border-left: 4px solid #9a3412; }
.term { border-bottom: 1px dashed #a8a29e; font-weight: 600; }
.t-title { font-size: 14px; font-weight: 650; fill: #1c1917; }
.t-sub { font-size: 11px; fill: #57534e; }
.t-label { font-size: 12px; fill: #1c1917; }
.t-micro { font-size: 10px; fill: #78716c; }
.t-white { font-size: 12px; font-weight: 600; fill: #ffffff; }
@media (max-width: 600px) {
  .post-title, .post-content h2, .post-content h3, .post-content h4 { text-wrap: balance; }
  .post-content p { text-wrap: pretty; }
  .post-content table { display: block; overflow-x: auto; }
  .fig-card--dense { overflow-x: auto; padding: 16px; }
  .fig-card--dense .fig-svg { min-width: 680px; }
  .fig-card--dense .fig-caption { min-width: 680px; }
}
</style>

第一篇的世界建立在三块基石上：**数据已经到齐**，有界输入，算完即止；**依赖关系可以画成图**，循环则用回边加进度协议表达；**机器不会坏**，所以第一篇从头到尾不需要讨论恢复。

真实流计算把其中的两块基石直接抽掉——数据与机器。数据永远不到齐：它持续到达，没有结尾。机器会坏：跑了三个月的算子，说没就没。本篇的结构就是依次打破这两个假设，而状态恰好坐在两个假设的交点上：它是无界计算的记忆，也是故障发生时要抢救的东西。

在动笔之前，先把全篇唯一的例子展示在这里。七笔订单，schema 是 `(order_id, user, amount, event_time)`：

| 订单 | 用户 | 金额 | 时间 | 应属窗口 |
| --- | --- | --- | --- | --- |
| o1 | U1 | 10 | 1 | W1 |
| o2 | U2 | 20 | 2 | W1 |
| o3 | U3 | 30 | 3 | W1 |
| o4 | U4 | 40 | 5 | W2 |
| o5 | U5 | 50 | 6 | W2 |
| o6 | U6 | 60 | 7 | W2 |
| o7 | U7 | 70 | 8 | W2 |

计算任务是两个事件时间滚动窗口上的订单数与 GMV。**窗口约定：左闭右开**，每扇窗包含左端点、不包含右端点：W1 = [10:00, 10:05)，W2 = [10:05, 10:10)。事件时间达到 10:05 的订单属于 W2，10:05 是两扇窗之间干净的分界：o3（事件时间 10:03）落在 W1，o4（事件时间 10:05）是 W2 的第一笔订单，因为窗口不包含自己的右端点。正确答案固定不变：W1 应有 C=3、GMV=60；W2 应有 C=4、GMV=220。

数据源按事件时间升序发出这七笔订单。但 o3 在手机上多滞留了一会儿，o5 来自一部离线手机，于是系统实际看到的到达顺序是：

```text
到达顺序：o1, o2, o4, o3, o6, o7, …, o5
                                      ↑ 最后才到
```

<figure class="fig-card fig-card--dense">
<svg class="fig-svg" viewBox="0 0 760 330" role="img" aria-label="七笔订单的事件时间轴与到达时间轴">
<defs>
<marker id="ex-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#57534e"/></marker>
</defs>
<text x="380" y="25" text-anchor="middle" class="t-title">同一批订单的两条时间轴</text>
<rect x="110" y="52" width="340" height="30" rx="6" fill="#f0fdfa" stroke="#99f6e4"/>
<rect x="450" y="52" width="300" height="30" rx="6" fill="#fff7ed" stroke="#fdba74"/>
<text x="280" y="71" text-anchor="middle" class="t-sub">W1 = [10:00, 10:05)</text>
<text x="600" y="71" text-anchor="middle" class="t-sub">W2 = [10:05, 10:10)</text>
<line x1="60" y1="130" x2="740" y2="130" stroke="#57534e" stroke-width="1.5" marker-end="url(#ex-arrow)"/>
<text x="60" y="118" class="t-label">事件时间</text>
<g>
<circle cx="150" cy="130" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="150" y="134" text-anchor="middle" class="t-label">o1</text>
<circle cx="230" cy="130" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="230" y="134" text-anchor="middle" class="t-label">o2</text>
<circle cx="310" cy="130" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="310" y="134" text-anchor="middle" class="t-label">o3</text>
<circle cx="470" cy="130" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="470" y="134" text-anchor="middle" class="t-label">o4</text>
<circle cx="550" cy="130" r="13" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.5"/><text x="550" y="134" text-anchor="middle" class="t-label">o5</text>
<circle cx="630" cy="130" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="630" y="134" text-anchor="middle" class="t-label">o6</text>
<circle cx="710" cy="130" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="710" y="134" text-anchor="middle" class="t-label">o7</text>
</g>
<line x1="60" y1="240" x2="740" y2="240" stroke="#57534e" stroke-width="1.5" marker-end="url(#ex-arrow)"/>
<text x="60" y="228" class="t-label">到达时间</text>
<g>
<circle cx="120" cy="240" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="120" y="244" text-anchor="middle" class="t-label">o1</text>
<circle cx="200" cy="240" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="200" y="244" text-anchor="middle" class="t-label">o2</text>
<circle cx="280" cy="240" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="280" y="244" text-anchor="middle" class="t-label">o4</text>
<circle cx="360" cy="240" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="360" y="244" text-anchor="middle" class="t-label">o3</text>
<circle cx="440" cy="240" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="440" y="244" text-anchor="middle" class="t-label">o6</text>
<circle cx="520" cy="240" r="13" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="520" y="244" text-anchor="middle" class="t-label">o7</text>
<circle cx="660" cy="240" r="13" fill="#ffedd5" stroke="#9a3412" stroke-width="1.5"/><text x="660" y="244" text-anchor="middle" class="t-label">o5</text>
</g>
<path d="M310 143 C330 190,340 195,358 227" fill="none" stroke="#9a3412" stroke-width="1.4" stroke-dasharray="4 4"/>
<path d="M550 143 C580 225,610 210,652 227" fill="none" stroke="#9a3412" stroke-width="1.4" stroke-dasharray="4 4"/>
<text x="340" y="196" class="t-micro">o3：小乱序</text>
<text x="560" y="200" class="t-micro">o5：离线手机，大迟到</text>
<text x="380" y="300" text-anchor="middle" class="t-sub">橙色虚线：离开自己时间位置的两笔订单；其余订单按序到达，连线从略</text>
</svg>
<figcaption class="fig-caption">事件时间轴上订单各就各位，到达时间轴上 o3 与 o4 互换位置，o5 远远掉队。后面所有推演表都以这两条轴上的固定数字为准。</figcaption>
</figure>

这七笔订单会贯穿全篇：四种进度机制各用它走一遍进度追踪，四种归属方案各用它存一次窗口状态，六种恢复策略在同一场宕机里各用它恢复一次。下面先打破第一个假设：数据永远不到齐，算什么、怎么算？

## 1. 从有界流到无界流：为什么需要一个人造结尾

在有界世界里，完整性是不言自明的。批处理读一个文件，读到结尾就知道输入已经完整，求和、join、去重都可以放心地输出定稿。Watermark 这类东西在有界流里根本不需要，文件结尾就是天然的完整性证明。

无界流没有这个结尾。这把一个从未存在过的问题摆上台面：如果坚持等全部输入到齐，**任何 join 和聚合都永远无法输出**；如果来一条算一条，系统又永远不知道一个答案何时可以定稿。一扇一小时的窗口、一笔等待另一侧的 join、一次去重，都要回答同一个问题：过去完整到哪里？

出路只有一条：系统自己制造边界，把无界输入切成可以推进的前缀。窗口是最常见的切法，epoch 和逻辑时间是别的切法。切完之后，结果就有了前缀的含义：

```text
R(T) = F({ e | EventTime(e) <= T })
```

一个结果声称自己代表时间 T 以前的世界，它就必须回答：还有没有 Event Time 不晚于 T 的订单正在路上？

可是，**切窗口这个动作本身制造了新问题：窗口何时算满？** 正因为我们要来了窗口，才有了乱序的问题。如果数据严格按事件时间到达，判断是平凡的：看到一笔越过窗口末端的订单就关窗。现实当然不是这样。这个问题一旦展开，会牵出六个追问：从数据看有四个，从机器看有一个，最后还有一个把前面所有问题收拢的问题。

**从数据看，四个问题：**
1. 无限数据怎样被切成可计算的有限部分？
2. 什么时候可以认为某部分结果完整？
3. 数据乱序、迟到时怎么办？
4. 输入被插入、删除或更正后，已发布结果怎样更新？——第 3 问的后续：迟到的数据越过边界，只能修订

**从机器看，一个问题：**
5. 机器故障后怎样恢复同一份结果？

**还有一个问题：**
6. 哪些历史数据必须作为状态保留？

乱序到达怎么处理。

## 2. 乱序：窗口完整性遇到的现实

要谈乱序，先得说清一条数据属于哪里。一笔订单至少牵涉三种时间：

| 时间域 | 本例中的取值 | 回答的问题 |
| --- | --- | --- |
| Event Time | o5 的支付发生在 t=6 | 业务事实什么时候发生，应归入哪个窗口 |
| Ingestion Time | o5 进入消息系统的时刻 | 数据什么时候进入流平台，入口积压了多久 |
| Processing Time | 算子实际处理 o5 的墙上时刻，重放（replay）时会变 | 某次执行什么时候处理了它 |

这三个时间是三种语义，不是每条记录上必然存在的三个物理字段。Event Time 通常来自业务字段或 source 分配的时间戳；Processing Time 是算子运行时读取的本地时钟；早期 Flink 曾把三者暴露为全局 `TimeCharacteristic`，从 Flink 1.12 起这个全局开关被弃用，改为由具体窗口、timer 和 `WatermarkStrategy` 显式表达。三分法作为分析框架仍然好用，但请不要把它想成数据库表里的三列。时间模型的第一项职责不是排序，而是<span class="term">归因</span>：一笔订单属于哪个窗口，一次修改属于哪个版本。

乱序的成因分两类。系统之外：网络路由的延迟差异，多数据源各自为政、合并后天然交错；本例的 o3 和 o5 都是外部原因。系统之内：并行 join 按 join 属性重分区后按匹配顺序输出，按非排序属性开窗，按非排序属性做优先级调度，对两个未同步的流做 union，每一种都会把整齐的流重新打乱。

后果可以用本例直接坐实。`map`、`filter`、`project`、去重、union 这类顺序无关算子，乱序来了也照算。窗口聚合不一样：如果系统看到 o4（ET=5）就认为 W1 该关了，W1 会输出 C=2、GMV=30，o3 的 30 块钱就丢了。这是第一个难题：对乱序坐视不管，结果就是在部分输入上算答案。对面还有第二个难题：不知道数据能晚到多久就无限等待，输出被堵死，状态无限囤积。乱序管理要做的，就是越过这两个难题。

把因果链摆出来：**无界 → 要切窗口 → 窗口要完整性判据 → 乱序让判据变难**。

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节解决“一条数据属于哪里，以及它什么时候算晚”。把归因规则（Event Time）、到达事实（Ingestion Time）、执行偶然（Processing Time）分开之后，乱序才从模糊的烦恼变成可以度量的问题：o3 偏了一个位置，o5 偏了两个位置。</p>
</div>

那么系统怎么回答“过去完整到哪里”？历史上先长出了两种截然不同的架构直觉。

## 3. 两种架构直觉：in-order vs out-of-order

假如数据总是按事件时间整齐到达，触发是平凡的，看到越过窗口末端的元组就关窗，这一章不需要存在。全部的分歧在于：到达不齐，怎么办？

**直觉一：in-order——入口处排好序。** 在系统入口缓冲并重排元组，直到一个迟到上界，然后把排好序的元组发给算子并清空缓冲。下游算子永远看到一条整齐的流，触发逻辑保持平凡。设上界为“最多容忍错位 1 个位置”，让本例的到达序走一遍：

| 到达 | 输入管理器的动作 | 发给算子的内容 |
| --- | --- | --- |
| o1, o2 | 位置正确，直接转发 | o1, o2 |
| o4 | 位置 3 空缺，入缓冲等待 | （无） |
| o3 | 补齐空缺；重排后转发，清空缓冲 | o3, o4 |
| o6, o7 | 位置 5 空缺；缓冲到 o7 时，o5 已错位 2 个位置，越过上界 | 宽限耗尽：仍未到的 t≤6 数据一律视为越界，转发 o6, o7 |
| o5 | 迟到越界 | 丢弃 |

W1 在被转发的 o4 触及窗口末端时触发：C=3、GMV=60，正确。代价写在入口处：缓冲的内存与延迟，以及越界数据被直接丢掉。

**直觉二：out-of-order——到达即算。** 不排队，元组到达即处理；由算子或一个全局权威产生进度信息，沿数据流图传播，作为迟到上界。设进度信息为“最大已见事件时间减 2”（下一节会正式称它为低水位线，low-watermark）：

| 到达 | 算子动作 | 窗口状态（W1 / W2） | 当前进度界 |
| --- | --- | --- | --- |
| o1 | 计入 W1 | {o1} / {} | maxET−2 = −1 |
| o2 | 计入 W1 | {o1,o2} / {} | 0 |
| o4 | 计入 W2 | {o1,o2} / {o4} | 3 |
| o3 | 3 不早于 3，合法迟到，计入 W1 | {o1,o2,o3} / {o4} | 3 |
| o6 | 计入 W2；进度界到 5，触发 W1 输出 | **W1 输出 C=3, GMV=60** / {o4,o6} | 5 |
| o7 | 计入 W2 | / {o4,o6,o7} | 6 |
| … | 后续订单陆续到达，进度界继续推进 | / {o4,o6,o7} | 7 |
| o5 | 6 早于进度界 7，越界 | 拒收（或进侧输出） | 7 |

两种方式算出的 W1 相同，路径完全不同：有序架构把复杂度放在入口缓冲区，乱序架构把复杂度放在“为迟到数据保留的状态”和一套进度协议上。

为什么第一代系统几乎一边倒地选直觉一，第二代又几乎一边倒地选直觉二？根子不在架构偏好，而在**谁规定数据的含义**。第一代流系统来自数据库研究，把流理解为一张随时间变化的关系表，schema 和算子语义由系统规定：

| 系统 | 数据模型要点 |
| --- | --- |
| STREAM | 流是“元组-时间戳对”的袋（bag），关系是随时间变化的元组袋；实现上统一成带插入/删除标志的带戳元组序列。**输入流只含插入，关系里才可能出现删除** |
| TelegraphCQ | 与 STREAM 类似的数据模型 |
| Aurora / Borealis | Aurora 把流建模为 append-only 元组序列，部分属性作 key；Borealis 推广为插入、删除、替换三种消息，消息还可以携带 QoS 相关字段 |
| Gigascope | 扩展序列数据库模型；元组带一个或多个时间戳/序列号，顺序属性可以（严格）单调递增或递减、单调不重复、或组内递增 |
| CEDR | 事件带有效时间戳 Vs（或有效区间）；时间 t 的关系内容 = 所有 Vs ≤ t 的事件 |

既然系统自己握有语义和查询计划，部署又以集中式、scale-up 为主，那么在入口处把序修好就是最省事的选择，反正本来就假设输入大体有序。第二代系统受 MapReduce 和云计算影响，不再对输入元素强加 schema 和语义，只要求元素带时间戳；流里的元素是插入、删除、替换还是差分，系统一概不管，语义由开发者负责。架构随之变成 shared-nothing 集群上的 scale-out：数据按分区并行进入，各分区天然不齐，入口修序从省事变成全局瓶颈，到达即算反而是顺势而为。

| 维度 | 第一代（关系流模型） | 第二代（数据流模型） |
| --- | --- | --- |
| 数据模型 | 随时间变化的关系，袋语义 + 插入/删除标志或有效时间 | 不解释内容的带戳元素，插入/删除/差分皆可 |
| schema 与语义归谁 | 系统规定，查询优化器全局优化 | 开发者负责，算子逻辑自定义 |
| 状态归谁 | 系统内部的 synopsis，用户不可见 | 从用户自理到“用户定义、系统托管” |
| 架构 | 集中式 / scale-up，假设输入基本有序 | shared-nothing scale-out，直面乱序 |
| 代表系统 | STREAM、TelegraphCQ、Aurora/Borealis、Gigascope、CEDR | Storm、MillWheel、Spark Streaming、Flink、Naiad |

乱序架构赢了，现代系统大多选直觉二。但它的命脉是那份“进度信息”：窗口何时触发、状态何时清理、迟到何时算越界，全押在它身上。

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答“乱序应该在系统的哪个位置被吸收”。入口处修序，代价是缓冲和延迟，收益是下游逻辑简单；到达即算，代价是状态保留和进度协议，收益是吞吐和扩展性。两种直觉的分歧不是工程口味，而是两代数据模型的自然延伸。</p>
</div>

这个信息从哪来？接下来先看三个实际使用的机制：Slack、low-watermark 和 PointStamp。它们分别用固定范围、系统进度和逻辑时间来回答这个问题。heartbeat 曾经是一种常见的源端进度信号，但它依赖数据源兑现承诺，已经逐渐退出主流实践；这里不再展开。

## 4. 三种进度机制：逐渐严谨的进度追踪

三种机制，同一份订单流，同一对窗口，只换进度信息的生产方式。先约定全篇规则：元组被接纳的条件是其事件时间**不早于**当前进度界；窗口在进度界达到窗口末端时触发。

### 4.1 松弛量（Slack）：拍脑袋的预算

Slack 是一个固定宽限。它最早按元组个数计量：一个乱序元组实际出现的位置，与它准时到达时应在位置之间隔了几个元组；也可以换算成时间。它只回答“容忍多少”，是用户写进查询规格的一个**写死的常量**——不随数据变化，不往下游发任何进度信号，也回答不了“旧状态什么时候可以丢”，只对每条到达的数据做一次“是不是迟到超过 K”的校验。设 slack = 1 个元组：

| 步骤 | 事件 | 算子行为 |
| --- | --- | --- |
| 1 | o1, o2 到达 | 计入 W1 |
| 2 | o4 到达（ET=5 触及 W1 末端） | 不触发，宽限内再等 1 个元组 |
| 3 | o3 到达（错位 1 位，在宽限内） | 计入 W1；宽限耗尽，触发 W1：**C=3, GMV=60** |
| 4 | o6, o7 到达 | 计入 W2 |
| 5 | o5 到达（错位 2 位，超出 slack=1） | 拒收 |

实现简单、行为可预测，但参数是死的：调小了丢掉合法迟到，调大了增加延迟和缓冲。

还有一个容易被忽略的软肋：**K 是全查询共用的常量，可每个算子拿来判断的本地观测却不一定一样**。上游算子可能已经见到 t=10 的订单，下游算子（中间隔着 filter 或聚合）可能只见到 t=8。于是在时间型 Slack 里，不同算子算出的"迟到上界 = 观测最高时间戳 − K"就不一致，甚至可能对同一条数据给出不同判断——Slack 没有一个所有算子共享的进度状态，判断是各说各话的。（按元组位移的那版例子里没有"最高时间戳"，算子只是按位移缓冲重排，但"本地判断、互不对齐"的性质是一样的。）低水位线靠 punctuation 传播一条统一的游标，恰好把这个各说各话抹平了——这正是 4.2 要讲的。

### 4.2 低水位线（low-watermark）：不看承诺看实测

对流的某个递进属性 A（通常是事件时间），low-watermark 是一个**沿数据流推进的游标**，它的语义是“到这个时间点为止的数据我都已经确认收到，**后面不会再有更早的**”——也就是系统内**最旧的待处理工作**在哪里。它由截断（punctuation，嵌入数据流的元数据元组）携带，沿算子一层层传播；有了它，算子既知道**这条数据收不收**，也知道**旧状态什么时候可以丢**。为了容忍乱序，源端会让游标比“已发出的最大事件时间”再退后一小段（本例退 2），退后的这段是安全边际；但会随数据推进的是游标本身，不是这个减数。这正是它和 Slack 的分水岭：**Slack 只有额度、没有游标；低水位线是有游标、额度只是修饰。**把两者并排看更清楚：

| | Slack | low-watermark |
| --- | --- | --- |
| 判断依据 | 每个算子用自己的本地观测 | 全链路共享、被推进的游标 |
| 不同算子会一致吗 | 可能不一致（各自观测不同） | 一致（同一条游标） |
| 有没有协调 | 无 | 有（punctuation 传播） |

设源端按“已发出的最大事件时间减 2”产生 low-watermark punctuation，punctuation 与数据走同一条通道：

| 步骤 | 到达算子的内容 | 算子行为 |
| --- | --- | --- |
| 1 | o1, o2 | 计入 W1 |
| 2 | o4；punctuation LW=3 | o4 计入 W2；进度界推进到 3 |
| 3 | o3 | 3 不早于 3，合法迟到，计入 W1 |
| 4 | o6；punctuation LW=5 | o6 计入 W2；LW 达到 W1 末端，触发：**C=3, GMV=60** |
| 5 | o7；punctuation LW=6 | o7 计入 W2 |
| 6 | …（后续订单与 punctuation） | LW 推进到 7 |
| 7 | o5 | 6 早于 LW=7，拒收 |

low-watermark 的含义是“系统内最旧的待处理工作在哪里”，它把进度概念延伸进了计算内部；并且可以从时间戳推广到任何递进属性，比如用序列号度量进度。

### 4.3 时空坐标（pointstamp）与前沿（frontier）：逐事件的精确账

pointstamp 不是独立的 punctuation，而是附着在每个元组上的 `(时间戳, 数据流位置)`。系统跟踪未处理事件之间的依赖：位置 p 处时间戳为 t 的未处理元组，若能在不晚于 t' 的时刻到达 p'，就“可能导致”pointstamp (t', p')；没有任何未处理元组可能导致的那些 pointstamp 构成 frontier，frontier 之内的通知可以交付。

在本例中，o3 还在路上时，pointstamp (3, 窗口算子) 处于活跃状态，窗口算子的 frontier 无法越过 t=3。于是 W1 的触发通知只会在 o3 真正被处理之后交付：**C=3, GMV=60，不猜，也不会错**。frontier 的本质不是“某时间之前的工作已完成”，而是“更早逻辑时间的工作不可能再出现”：前者在分布式系统里无从断言，后者却可以由未完成消息和算子持有的 capability 精确推出。

读者已经见过这套东西。第一篇的迭代计算里，上游用 capability 说明“我还可能发”，下游用 frontier 判断“我已经收完”；那里的 frontier 管的是 `(epoch, iteration)` 坐标下的迭代进度。pointstamp 就是它的完整形态：给进度信息加上数据流位置，再用“可能导致”关系把任意图形状（分支、合并、嵌套、循环）上的未完成工作算成一条下界。

循环数据流最能看出 pointstamp 的价值。用 punctuation 传递进度时，环里的二元算子（join 或 union）必须在两路输入都看到同一 punctuation 才能转发，否则阻塞等待；可它的一路输入来自自己下游的输出，等输出要先等输入，等输入要先等输出，死锁不可避免。pointstamp 把迭代次数收进逻辑时间：消息每绕一圈反馈边，iteration 分量加一，所有未处理事件在偏序上排队，frontier 照样推进，没有谁等谁的问题。

但有一个边界必须说清：这种精确只对已经纳入逻辑时间协议的内部计算成立。o5 的事件时间来自手机，是外部世界的事实；输入端仍然要自己决定何时宣布“t=6 之前的输入已完整”。一旦宣布，后来补到的更小 epoch 数据就没有别的出路，只能作为更晚逻辑时间上的撤回或更新来表达——它只能触发修订，这正是 4.6 要讲的。

同一份订单流在 Timely 里从头到尾走一遍。看代码之前，先弄清一件事：capability（证）从哪来是合法的。因为 Timely 判断"时间 t 完没完"靠的是**数还有几张 t 的证在外面**，所以证的来源必须管死，否则这个数就不可信：

| 合法来源 | 是什么 | 旧的还在吗 |
| --- | --- | --- |
| `input.capability()` | 建图时系统发的第一张证（最小时间），一切的起点 | — |
| `cap.delayed(&t)` | 从已有的证**复印**一张到更晚时间 | 在（留底） |
| `cap.downgrade(&t)` | 从已有的证**换发**一张到更晚时间 | 不在（销底） |
| 算子内 `retain()` / `InputCapability::delayed()` | 把刚收到的那条消息自带的证，转成自己输出口的证 | 在 |
| `handle.advance_to(t)` | 有序 `Handle` 的时钟推进：内部相当于把句柄那张**隐含证** downgrade 到 t（旧时间 −1、新时间 +1，外加 flush、关旧 epoch）——证不出句柄，用户拿不到 | 不在（销底） |

规则只有一条：**任何证都追根到系统发的第一张，且只能沿时间向前传**。没有凭空造证，没有往过去发证——所以"frontier 越过 t"才等于"t 真的完了"。另外注意：`session(&cap)` 的参数是证这个对象，不是裸时间戳；`event_time` 是数据的事实，证是你写这个时间的资格，事实必须凭资格入场。

弄明白证的来路之后，看源端程序——它只有一个 while 循环（基于 timely-dataflow 的 UnorderedInput；窗口聚合两行是示意，其余 API 与源码一一对应）：

```rust
// 建图：源端允许同时保留多个时间的 capability
let mut input = worker.dataflow::<u64, _, _>(|scope| {
    let (handle, stream) = scope.new_unordered_input::<Order>();
    stream.map(|o| (window_of(o.event_time), o.amount))
          .sum_by_window();              // 示意：§3 的 W1/W2 窗口聚合
    handle
});

// 源端只有一个 while 循环：从通道读一条，发一条。
// 没有人提前告诉你 o3 会来；你唯一能决定的，是 frontier 推到哪。
let mut cap = input.capability();        // 握着 frontier 的时间通行证
let mut max_seen = 0u64;

while let Some(order) = channel.recv() {
    // 每条数据用自己的事件时间发送：从 frontier 派生一个临时 capability
    let data_cap = cap.delayed(&order.event_time);
    input.session(&data_cap).give(order);

    // 更新 frontier：已见过的最大事件时间 − 2，给还在路上的迟到者留余地
    max_seen = max_seen.max(order.event_time);
    cap.downgrade(&(max_seen - 2));      // 只进不退
}
```

循环里唯一的自由度是 `downgrade` 推到哪：**推到 `max_seen`，等于立刻封死所有更早的时间**——o3 就再也进不来了；**减 2，就是给迟到者留的余地**——这正是 4.2 低水位线"最大已见减 2"的出处。要特别注意 `downgrade` 做的是什么：它是源端**本地的一句"锁定答案"**——"我担保这个时间之前的数据都发完了"。它不等任何人，不和其他 worker 握手，只是把本地计数改掉；全局 frontier 的变化，是这句声明经进度协议传播之后的**后果**，不是 `downgrade` 自己同步出来的。按时间顺序逐轮走读：

| 循环读到 | 这一轮做什么 | 内部实际发生了什么 | 此刻系统状态 |
| --- | --- | --- | --- |
| o1（t=1） | 派生 data_cap@1，发出 o1 | 消息盖 t=1；pusher 的 Counter 记 produced +1@1 | W1={o1}；frontier=0 |
| o2（t=2） | 同上发 o2 | max_seen=2；cap 仍停在 0 | W1={o1,o2} |
| o4（t=5）先到 | 派生 data_cap@5，发出 o4 | `give` 只校验这张 cap 属于这个输出口，**不比时间大小**；max_seen=5，`downgrade(&3)` | W2={o4}——先算，不等 o3；frontier=3 |
| o3（t=3）后到 | 派生 data_cap@3，发出 o3 | 3≤3 成立，盖 t=3；max_seen 不变，cap 不动 | W1={o1,o2,o3}，合法，不算乱序事故；frontier=3 |
| o6（t=7） | 派生 data_cap@7，发出 o6 | max_seen=7，`downgrade(&5)`：t=5 以下的计数归零 | frontier=5 → **W1 触发：C=3, GMV=60** |
| o7（t=8） | 发 o7 | max_seen=8，`downgrade(&6)` | W2={o4,o6,o7}；frontier=6——**注意此刻 t=6 还开着**（6 ≥ 6） |
| …又来一单（ET=9） | 派生 data_cap@9，发出 | max_seen=9，`downgrade(&7)` | frontier=7——**t=6 的门在这一刻关上** |
| o5（t=6）最后才到 | 想派生 data_cap@6 | 上一行源端已 `downgrade(&7)`——这等于宣布"系统只接受 ≥7 的时间了"；o5 是 6，`try_delayed(&6)` 返回 `None`（`delayed` 直接 panic） | t=6 写不进去了 |
| 岔路 | `diff.update_at((W2, +50), 新 epoch, +1)`，再 `advance_to`、`flush()` | `update_at` 断言新 epoch 不早于当前 session 时间，把三元组攒进 buffer；`advance_to` 只翻本地时钟；`flush` 才真正发出 | 新 epoch 上多一条 diff：意思是"现在修正过去"，不是把 o5 改到 t=6 |

注意这张表里根本没有"知道 o3 会来"这回事：o3 能进来，靠的不是预知，而是 `downgrade` 推到 3 就停住了——o4 到的时候 frontier 还在 3，旧时间仍然开放。这也是和第 3 章两张推演表的对照点：in-order 架构靠缓冲让 o4 等 o3，out-of-order 的 watermark 靠"最大已见减 2"的猜测推进，Timely 则是**先算、不等、但 frontier 不松口**——o4 的提前计算和 W1 的延期关闭互不干扰。一句话读法：**frontier 没越过之前**，迟到的 o3 想什么时候进来都行；**frontier 一旦越过**，旧时间的通行证作废，o3 只能变成新 epoch 里的一条 diff。

### 4.4 三种机制对比

| 维度 | Slack | low-watermark | pointstamp/frontier |
| --- | --- | --- | --- | --- |
| 进度证据 | 固定宽限，无证据 | 最旧待处理工作 | 未完成事件的精确依赖 |
| 载体 | 查询参数 | punctuation | 每个元组自带 |
| W1 触发时机 | o3 到达且宽限耗尽 | LW=5 到达 | frontier 越过 t=3 |
| 本例 W1 结果 | C=3, GMV=60 | C=3, GMV=60 | C=3, GMV=60 |
| o3 的命运 | 宽限内收下 | 收下 | 必然被等待 |
| o5 的命运 | 越界拒收 | 拒收 | 取决于输入端的进度声明 |
| 循环数据流 | 不支持 | punctuation 模型在环中互相等待，死锁 | 原生支持 |

### 4.5 从 Storm 到 Timely：进度机制一代比一代精确

前面三个机制是工具箱；要看它们被哪个系统采用、为什么，得按系统的演进讲。每一代系统不是为了炫技术，而是补上上一代留下的具体窟窿。

**① Storm（第一代）：先保证"消息没丢"，但不管"时间到哪了"。** Storm 要解决最朴素的问题：一条源消息被展开成成千上万个 tuple，分散到各台机器处理，怎么保证它们都被算到了、没有丢。答案是用 XOR acker 做 tuple tree 的完成检测，配可靠 spout 超时重放，拿到 at-least-once（第 6 章细讲）。但它回答的是"这条源消息派生的工作确认完了吗"——一个**消息级**的问题；它没有"事件时间推进到哪儿"的概念，也没有窗口何时该关的说法。后来的 Windowing API 用 lag（一种 heartbeat）补出一个很粗的事件时间窗口，那是补丁，不是核心。**它留下的窟窿**：只有消息确认，没有窗口进度。

**② Spark Streaming：用"切批"粗粒度地补上进度。** Spark Streaming 换了条路：不把流当一条条数据，而是切成一个个 micro-batch。一批之内数据是有限的、一起到的，"这批算完了"就是天然的进度信号——就像有界流免费得来的"文件结尾"（第 1 章那个完整性证明）。于是它不需要 watermark。**它留下的窟窿**：输出延迟是批级的，由 batch interval 决定；批边界按"到达时间"切、不按"事件时间"切，对乱序和事件时间语义支持很弱。要低延迟、要逐事件、要事件时间，微批就力不从心了。

**③ Flink：要逐事件低延迟，就必须有 watermark。** Flink 不切批，真正来一条算一条。可这样一来，就没有"批结束"这个免费信号了，必须自己回答"过去完整到哪"——于是 watermark 成了核心。常见的 bounded-out-of-orderness 策略在每个 source 分区上维护：

```text
LocalWM_i = MaxEventTime_i - B
```

B 是允许的乱序幅度，取 B=2 时，4.2 那张表的每一步就是 Flink 单分区下的行为。这个公式恰好揭示了机制的分层：**减号后面那个 B 就是内嵌的 slack，减号前面会随数据动的 `MaxEventTime` 才是低水位线的游标**——所以 Flink watermark 本质是低水位线，只是游标里嵌了一个 slack。多分区时，算子对所有活跃输入通道取最小值：

```text
OperatorWM = min(LocalWM_1, ..., LocalWM_n)
```

这里不存在"全作业唯一"的中心 Watermark；每个算子实例根据自己的活跃输入各自计算，进度逐层传播。一路输入停住会拖住全局，所以要 idleness 把长期无数据的分区暂时排除；watermark 随数据流传播，不会越过排在自己前面的元组。**它留下的窟窿**：watermark 是"估算"出来的（那个 B 是拍的安全边际），声明的其实是"我猜不会再有更早的了"；而且它是标量、沿 punctuation 传，遇到循环（feedback 回边）就死锁（4.3 讲过）。

**④ Timely / Naiad：连估算都不要，精确追踪。** Timely 把"未来还可能出现哪些逻辑时间的工作"直接算出来：每条元组带 `(时间戳, 位置)` 的 pointstamp，系统汇总成 frontier。不需要 slack，原生支持循环和嵌套——这就是 4.3 的精确账；第一篇的股权穿透迭代也是在这套 `(epoch, iteration)` 坐标下跑的。代价是实现和理解门槛更高，所以它主要是研究系统，而 Flink 把"估算式 watermark"做成了工程主流。

| 维度 | Storm | Spark Streaming | Flink | Timely/Naiad |
| --- | --- | --- | --- | --- |
| 进度的载体 | 消息确认（ack） | 批边界 | 估算式 watermark | 精确 frontier |
| "过去算完了吗"怎么判 | 源消息派生的工作都 ack 完 | 一批处理结束 | watermark 越过窗口末端 | frontier 越过对应逻辑时间 |
| 本例的 o3 落到哪 | 无事件时间窗口，只保证被处理 | 按到达时间落入某 micro-batch | watermark 内收下 | 必然被等待 |
| 循环 / 嵌套 | 无通用偏序进度模型 | 批内无循环概念 | 环中互相等待，易死锁 | 原生支持 |

沿这四个系统看下来，进度机制在同一条轴上变精确：Storm 只有消息确认，Spark 用批边界当粗进度，Flink 用估算式水印（低水位线 + slack），Timely 用精确前沿。每前进一步，能表达的场景越复杂，代价也越高。

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答"系统凭什么宣布过去完整"。四代系统在同一条轴上往前爬：从消息确认，到批边界，到估算式水印（低水位线 + slack），到精确前沿。每前进一步，都补掉了上一代"说不清过去完整到哪"的具体窟窿，也各自付出新的代价。</p>
</div>

可是再准的进度界也不是物理定律：越界的数据总会来。下一笔订单 o8 到达时，窗口已经输出过了，怎么办？

### 4.6 越过边界：迟到（lateness）、修订（revision），以及修订的本钱

回到本例。假设使用 Flink 风格的语义，W1 在进度界达到 5 时已经输出 C=3、GMV=60，但配置了 `allowedLateness`，窗口状态保留到进度界越过 5+5=10 为止。注意这个参数的含义：不是“第一次输出之前多等五分钟”，而是“首次照常触发，之后继续保留状态到进度界越过窗口末端加 5”。现在又来一笔 o8：ET=2、金额 25，是另一部离线手机姗姗来迟的订单。若它到达时进度界为 6（未越过 10）：

| 步骤 | 事件 | W1 状态与输出 |
| --- | --- | --- |
| 1 | 进度界到 5 | 首次触发：C=3, GMV=60；状态保留 |
| 2 | o8 到达（2 属于 W1） | 计入 W1，再次触发：**C=4, GMV=85** |
| 3 | 进度界越过 10 | 状态清除；此后到达的 W1 数据只能丢弃或进侧输出 |

已发布的 60 要改成 85，输出形式有三种，选哪种取决于下游认哪种：

```text
追加增量：   +25
主键覆盖：   60 -> 85（upsert）
撤回再插入： -60, +85（retract）
```

如果 sink 只支持 append，它看到 60 和 85 两个完整快照（snapshot）时无法判断该相加还是该覆盖。所以修订（revision）从来不只是计算引擎的事，它是引擎与下游之间的一份协议。工程上长出来过三条实现路线：

| 路线 | 做法 | 代表系统 |
| --- | --- | --- |
| 存储并 revision | 缓冲或存储流数据，增量地 revision 已捕获的值 | CEDR、StreamInsight、Google Dataflow |
| replay 并 revision | replay 受影响的历史子集，传播差分 revision 消息 | Borealis（动态 revision，历史有界）、推测性处理 |
| 分区并合并 | 按迟到程度分多个分区各自处理，再合并局部结果 | Truviso（顺序无关处理）、Trill（急迫排序） |

三条路线买同一个东西：先给出当前最好的结果，再吸收迟到与更正。而这份能力的本钱，全部写在状态里：窗口要一直保存累加器（accumulator）或原始记录，join 要保留两侧索引，去重要保留见过的 key，已发布结果的每次变化还可能连带更新下游的聚合、缓存和 sink。revision 半径越大，保留的历史越长，存储与重算越贵；大规模 revision 的状态量至今仍是工程难题。Watermark 与 allowed lateness 合起来划出一条工程边界：边界以内，系统愿意为过去保留 revision 能力；边界以外，状态可以删除，结果才真正封账。

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答“算早了怎么办”。revision 是低延迟与最终正确性之间的标准交易：先出当前最好的结果，再用 upsert、retract 或差分吸收迟到。交易的成本写在状态里，交易的对手方是下游，它必须听得懂更新和撤回。</p>
</div>

状态这个词已经出现了很多次：revision 的本钱是它，乱序架构的代价是它，第一节的六个问题里有两个直指它。它到底是什么、归谁管、按什么形状存、多久落一次盘？这就是下一章。

## 5. 状态：历史影响的物化

状态从哪来？从跨事件的关联来。`map` 和 `filter` 逐条工作，上一条订单怎么处理与下一条无关，不需要记住任何东西。窗口、join、distinct 不一样：第 k 批的输出依赖第 1 到 k 批的全部历史，关联必须有个存放的地方，这个存放处就是状态。

第一篇开头的求和树里，中间结果只是几个可合并的部分和；如果算的是平均值，中间结果就是一个固定大小的 (sum, count) 对。那是有界世界的中间态：计算开始才出生，输出那一刻就被扔掉；完整性与生俱来，文件结尾就是证明；失败了把查询重跑一遍就是。所以在有界世界里，“状态管理”这个概念根本不存在。无界世界里，同一个 accumulator 的命运完全不同：

| 维度 | 有界中间态 | 无界状态 |
| --- | --- | --- |
| 生命周期 | 计算期间临时存在，输出即抛 | 常驻，未来任何时刻的输入都可能需要它 |
| 完整性信号 | 文件结尾，与生俱来 | watermark/frontier，需要人造 |
| 清理 | 不需要 | 进度界、TTL、业务声明 |
| 失败含义 | 重跑查询即可 | 输入早已流过，不能从头再算，必须恢复 |

中间态从一次性用品变成必须管理的资产，这就是本章的全部缘起。

<div class="callout callout--insight">
<p><strong>本章的定义</strong>：只要某个信息会影响未来输入的处理结果，它就是状态。窗口的 accumulator、join 的缓冲区、distinct 见过的集合、Differential 的 arrangement，都是这同一个定义的实例。</p>
</div>

不过“状态”这个词到目前被混着用，至少指三件不同的东西，先把它们命名分开：

| 名字 | 内容 | 归哪章管 |
| --- | --- | --- |
| 工作状态 | 算子逐条读写的累积量：W1 的 (C=3, GMV=60)、join 缓冲、见过的 key | 本章 |
| 恢复状态 | 为故障准备的工作状态 snapshot，外加输入进度 | 第 6 章 |
| 输入/输出进度 | 输入侧的 offset 与 watermark/frontier，输出侧的已提交事务 | 第 4 章（输入）、6.3（输出） |

分开命名不是抠字眼。本章之前那些含糊（checkpoint 里装的到底是什么、watermark 管的又是哪一种），都来自这三件东西共用一个名字。

### 5.1 每个算子的最小历史

定义是判定标准，落到具体算子头上，必须记住的历史各不相同。到上一节末，我们的窗口算子手里一直攥着两样东西：W1 的 accumulator（C=3, GMV=60，触发后仍保留到进度界越过 10）和 W2 的 accumulator（{o4,o6,o7}，C=3, GMV=170）。把它们推广开：

| 算子 | 必须记住什么 | 什么时候可以删除 |
| --- | --- | --- |
| 窗口聚合 | 每个 key/window 的 accumulator，必要时还有原始记录 | 进度界越过窗口末端加 allowed lateness |
| 双流 join | 两侧按 join key 组织的历史或未匹配记录 | 时间条件证明另一侧不再可能匹配 |
| distinct | 已见 key 的集合或近似结构 | TTL/版本边界/业务声明允许忘记时 |
| Differential join | 两侧 arrangement 与尚未 compact 的时间差分 | frontier 允许合并历史时间差异时 |

### 5.2 归谁管：同一个累加器的四个主人

同一个 W1 accumulator，在不同的系统里有不同的主人：

| 时代 | 状态的主人 | 本例中的样子 |
| --- | --- | --- |
| 第一代 DSMS | 系统内部的 synopsis，用户不可见 | STREAM 为窗口算子挂一个内部概要结构，与 join 索引、源端缓冲并列；synopsis 之间还能组合复用 |
| Storm / S4 时期 | 用户完全自理 | 你在 bolt 里 new 一个 HashMap，或者自己接 Redis；持久化、扩容、恢复全是你的事 |
| 现代系统 | 用户定义、系统托管 | 你声明 `ValueState<(count, sum)>` 或窗口状态，系统掌握类型、序列化器和读写操作，据此提供持久化与容错 |
| Differential Dataflow | 系统性的差分 | 没有“窗口 accumulator”这个概念，只有 `(窗口键, 时间, diff)` 的集合和按 key 组织的 arrangement，状态是历史差分的积分 |

前三行是流系统史上真实发生过的三次交接，第四行（Differential）是本文补入的另一种形态。三次交接各失一物：第一代失表达力，只能是关系操作的子集；Storm 时期失一切系统支持，用户要自己想清楚持久化、超内存扩展和第三方存储依赖；现代的“用户定义、系统托管”失对数据结构的直接控制，自定义优化的空间被让渡给了引擎。第四行背后的世界观完全不同，留到 6.5 收束本章时细讲。

### 5.3 按 key 还是按全局

同样的 GMV 聚合，状态可以有两种形状。**按用户统计**：`keyBy(user)` 之后，U1 到 U7 各自的 accumulator 是逻辑上按 key 分区的状态，多个 key 范围可以落到不同物理任务上，并行度随 key 空间扩展，这是第二代系统扩展性的基石。**全局 GMV**：全流一个 accumulator，映射为单个物理任务上的单例状态，所有订单都要经过它，无法分区扩展。非分区状态另有正当用途，比如算子级指标或记录 source 消费的 offset，但它要么管算子局部、要么管全局聚合，都不可扩展，慎用。本例里，按用户聚合时 o5 只影响 U5 那一格状态；全局聚合时它要排队经过全系统唯一的热点。算的是同一笔账，状态的形状决定能不能 scale。

### 5.4 按条还是按 epoch 落盘

状态有了主人和形状，接下来的问题是多久持久化一次。注意从这里开始，落盘的产物就不再是工作状态本身，而是为故障准备的第二种状态：恢复状态。让 o3 的处理过程在两种粒度下各走一遍。

**记录级**：MillWheel 把每次本地动作当作一个事务提交到 BigTable，动作包括输入事件、状态转移和产生的输出，称为 strong production。本例中处理 o3 就是一次原子提交：

```text
输入：o3
状态转移：W1 (C=2, GMV=30) -> (C=3, GMV=60)
输出：无（窗口未触发）
```

由于提交时动作顺序已定，这种方案还附带保证确定性执行。代价是每次输出的提交延迟，靠 WAL、blind write、bloom filter、批量提交这些数据库祖传优化压回去。

**Epoch 级**：把计算切成一段段 epoch，每个 epoch 结束后提交整个任务图的状态。严格两阶段的做法是阶段一处理完整个 epoch、阶段二持久化状态，任务要互相等待，Spark Streaming 的微批（micro-batch）、Trident 的批次事务和 S-Store 的“每 epoch 一个 ACID 事务”都属此类，Drizzle 把多个 epoch 链成一次提交以缓解空等。异步的做法是 Chandy-Lamport 式的一致快照（consistent snapshot）：marker 随数据流插入，切出一致切，不暂停数据流。异步又分两派：非对齐 snapshot 运行时最快，但要把在途数据记入 snapshot，恢复时多一段 redo；对齐 snapshot 在 marker 处阻塞已到达的输入通道直到各通道齐平，提交更慢，恢复快，snapshot 恰好对应一个完整 epoch，还便于在对齐间隙做在线重配置。

本例中 Flink 在 o3 处理完后完成 checkpoint #42。用本章的词汇拆开它的成分，恢复状态恰好等于工作状态 snapshot 加输入进度：

```text
CP#42 = { 输入进度: source offset 已消费到 o3;
          工作状态: W1 (C=3, GMV=60) 未触发, W2 (C=1, GMV=40) }
```

两种粒度各买各的：记录级买逐条的确定性与快速恢复，付每条一次的提交；epoch 级买正常路径上几乎零开销，付故障后 replay 一个 epoch 的输入。

### 5.5 两种状态观：算子持有的东西 vs 流的一部分

到这里为止，本章默认了一种观点：状态是算子持有的东西。它要有地方放（内存、RocksDB、远端存储），要有规矩管（TTL、进度界清理），要有 snapshot 保命（checkpoint）。Flink 和现代系统都站在这一边，状态管理因此是一门独立的学科。

Timely 加 Differential Dataflow 的世界提供了另一种观点：**加上 pointstamp 和 diff 之后，所有的状态都是流的一部分，状态根本不是一个独立的东西。** pointstamp 给出时间维，diff 给出变化维：插入一条记录是 `diff=+1`，删除是 `diff=-1`，revision 是更晚逻辑时间上的一条新 diff，第 4.6 节的 upsert 和 retract 都是它的特例。accumulator 不过是差分流的积分；历史上每一个被“记住”的事实，都是一条还躺在流里的旧 diff。

这种观点下，要管理的不再是“状态”本身，而是两样派生物：compaction（frontier 推进之后，把已无区别能力的历史差分合并压缩）和 arrangement（按 key 组织的索引，让未来的差分能快速查询历史）。以双流 join 为例，两侧各自变化时：

```text
Delta(A JOIN B)
  = DeltaA JOIN B + A JOIN DeltaB
  + DeltaA JOIN DeltaB
```

一条新的 DeltaA 到达时，系统必须查询已经积累的 B；双线性让每次变化可以增量传播，但历史并没有凭空消失，它被物化成两侧 arrangement。这里守住 4.4 立下的分层：Timely 提供逻辑时间、消息调度与 frontier，Differential Dataflow 在其上提供 `(data, time, diff)`、增量算子与 arrangement。说“Timely 的 join 每来一条数据都做增量计算”并不准确，双线性增量 join 是 Differential 这一层的代数与实现。

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答“状态到底是什么”。它来自跨事件的关联；有界世界里它是一次性中间结果，无界世界里它必须常驻并被管理。判定标准只有一句：会影响未来输入处理结果的信息就是状态。把它分成工作状态、恢复状态、输入输出进度三件东西之后，第 4 章和第 6 章各自的管辖范围也就清了。</p>
</div>

无论站在哪种观点，工作状态都活在进程内存或本地状态后端里，进程一死它就没了。所以第二种状态（恢复状态）和第三种（进度）必须存在：这就是下一章，也是第一篇第二个假设被打破的地方。

## 6. 从理想机器到现实机器：无状态可重放，有状态要一致切

第一篇的第二个假设是机器不坏，现在打破它。分三步走，每一步都把问题逼到下一步。

**第一步：假设计算全部无状态。** 如果流水线里只有 map 和 filter，每条记录独立处理，机器坏了怎么办？答案简单到近乎无聊：replay 输入就行。无状态算子不积累任何东西，换台机器从断点接着读，结果分毫不差。无状态系统几乎不需要容错理论，输入日志本身就是全部恢复手段。

**第二步：但有些计算做不到无状态。** 窗口 accumulator、join 缓冲、distinct 集合，就是上一章那张表里的每一行。机器一坏，积累的历史随进程消失；光 replay 一条新输入没有用，因为新输入的含义取决于旧输入留下的状态。

**第三步：所以要把状态和输入位置一起拍下来。** 给“输入位置加全部算子状态”拍 consistent snapshot，机器坏了就从 snapshot 恢复状态、从记录的输入位置 replay。Checkpoint 的存在理由至此才出场：它不是性能优化，是有状态流计算敢承诺正确性的前提。

用第五章的词汇把这一步说精确：checkpoint 拍下的是<span class="term">恢复状态</span>，等于一致切（consistent cut）上的工作状态加输入进度（source offset）。恢复就是把这两样装回去：加载 snapshot、对齐 offset、replay 缺口。而 6.3 要讲的输出提交问题，本质是第三种状态出了错：输出进度（外部世界已经看到了什么）与恢复状态（系统能回滚到哪里）对不上。

### 6.1 先定义“对了没有”：三档处理语义

故障面前，系统的行为分三档：

| 语义 | 含义 | 本例中的表现 |
| --- | --- | --- |
| at-most-once | 故障可能丢数据 | o6 之后崩溃，o6 的 60 块钱可能从 W2 里消失 |
| at-least-once | 结果与无故障执行一致，外加恢复产生的重复 | o6 被 replay，W2 可能把它算两遍：GMV 多出 60 |
| exactly-once | 分两档：on state 指状态与不故障时一致；on output 指对外输出也与不故障时一致 | 状态账对了，但下游可能收到两次 W1 的结果 |

exactly-once on state 有一个常被忽略的假设：计算是确定性的。处理时间窗口、多源输入的算子都是不确定性的来源，恢复时状态可能分叉；Clonos 这类工作靠持久化不确定性事件的“决定因子”（determinants）来 replay 出一模一样的状态。

### 6.2 同一场崩溃，六种恢复

现在安排那场崩溃：**worker 在处理完 o6 之后、o7 到达之前宕掉**。此刻的账面是：已按到达顺序处理 o1, o2, o4, o3, o6；W1 已输出 C=3、GMV=60；W2 accumulator 为 (C=2, GMV=100)；最近可用的恢复状态是 CP#42：一份工作状态 snapshot 加一份输入进度（见 5.4）。六种恢复策略各自登场：

| 策略 | 恢复过程 | 本例的恢复结果 | 代价 |
| --- | --- | --- | --- |
| 上游备份（upstream backup） | 无 snapshot；上游节点/源端保留已发元组，故障后从保留处 replay | replay o1..o6 重建两个 accumulator，再正常处理 o7、o5；结果正确但恢复最慢 | 正常路径开销最低，恢复时间最长 |
| 主动备援（active standby，Flux 式） | 两份相同计算并行运行并协调进度，主死即切 | 副本手里已有 W2=(C=2, GMV=100)，几乎零恢复时间 | 双倍资源，副本间要持续协调 |
| 被动备援（passive standby，checkpoint） | 从 CP#42 恢复状态与 source offset，replay 之后的输入 | 状态恢复为 W1=(3,60) 未触发、W2=(1,40)；replay o6 后 W1 再次触发，**W1 结果对外重复**，撞 6.3 的输出提交问题 | 正常路径近零开销，恢复需 replay 一个 epoch |
| 备份服务器（Hwang et al.，被动的一支） | 各服务器把 checkpoint 状态的独立分片送往多台备份机 | 故障后多台备份机各自加载分片、并行拉起算子，恢复比单点被动更快 | 状态要预先分片搬运 |
| Borealis 式切换 | 下游节点改连故障上游的活副本；无副本时对不完整输入先发暂定输出（tentative output） | 看板先看到基于不完整输入的 W2 数值，恢复后再被修正 | 用一致性换可用性，保证最终一致 |
| Storm 逐条 ack/replay | 无 snapshot；acker 用 XOR 跟踪 tuple tree 是否完成，o6 的在途树超时后由可靠 spout replay o6 | bolt 内存里的 W2=(C=2, GMV=100) 随进程消失，协议不重建状态：o6 被 replay 进空 accumulator 得 (C=1, GMV=60)，再收 o7 得 (C=2, GMV=130)，永久错误（无故障应为 C=3, GMV=170）。状态若放外部存储，对错全看用户自己的幂等（idempotent）/事务协议 | acker 只需定长空间、不保存任何恢复数据；但完全没有状态恢复：恢复的是消息，不是状态 |

这几种方案的取舍，有人系统地建过模。Hwang 等人的经典结论是：主动备援恢复近零但资源开销最高；被动备援两项指标都差一些，却是任意查询网络都能用的唯一选项；上游备份正常开销最低、恢复最慢；还有不做任何准备的 amnesia 作对照。混合路线试图兼得：平时被动、检测到瞬时故障时启用预部署副本，或者按算子逐个选择主动复制还是上游备份，把故障时的峰值延迟压在阈值之下。代际之间也有明显的口味迁移：早期系统把高可用放在首位，偏爱主动复制，允许近似结果，靠保存输出元组来重发给下游；现代系统偏爱被动复制和云上按需资源，坚持状态上的 exactly-once，replay 则越来越依赖可 replay 的输入源。

### 6.3 输出提交问题

上表被动备援那一行暴露了一个独立的问题，它有自己的名字：<span class="term">输出提交问题</span>（output commit problem）。用第五章的词汇说，这是输出进度与恢复状态对不上：状态可以回滚到 consistent snapshot，输出不行，结果一旦发布给外部世界，就无法撤回。本例中，崩溃前 W1 的结果 C=3、GMV=60 已经发给下游看板；从 CP#42 恢复后 replay o6，进度界再次到 5，W1 再次触发，看板上出现第二个 GMV=60。状态完全正确，输出却重复了。解法有五大类：

| 路线 | 做法 | 代表 | 假设 |
| --- | --- | --- | --- |
| 基于事务 | 每条记录/每批带唯一 id，重试时按 id 去重 | MillWheel（记录 id + 高可用存储）、Trident（有序事务 id） | 外部高吞吐事务存储；确定性计算与输入、事务有序（Trident） |
| 基于进度 | 用时间戳/向量时钟识别 replay，下游按时间戳丢弃重复 | Seep | 确定性计算，单调逻辑时钟，记录按时间戳有序 |
| 基于血缘 | 记录算子间的输入输出依赖，恢复时沿依赖重建 | Timestream（逆拓扑序计算可回收依赖）、StreamScope（用 low-watermark 回收旧依赖） | 确定性计算与输入 |
| 特殊 sink | 可从文件/数据库撤回输出的 sink | IBM Streams | 输出可撤回，只适用特定场景 |
| 外部 sink | 把去重外包给支持 idempotent 写或事务的外部系统 | Flink、Spark 配 Kafka/事务型数据库 | 输出 idempotent 或可事务提交 |

基于进度与基于血缘这两类有个共同软肋：它们都靠下游算子过滤重复，而图上最后一个算子没有下游，最终输出的去重无人负责。后两类则可以用数据库的眼光重新归并：乐观输出先发后撤，像 MVCC；悲观输出先写日志再提交，像 WAL。实践中现代系统的主流选择是最后一行：状态内 exactly-once，输出去重外包给 idempotent 或事务型 sink。

### 6.4 从 Storm 到 Flink：为什么逐条重放做不到 exactly-once

六种方案里最值得关注的是 Storm 那一行：逐条 replay 为什么天生够不到 exactly-once？把 exactly-once on state 拆成两个前提，答案就自己浮出来了：

1. 一个可回滚的 consistent cut：同一时刻的输入位置加全部算子状态；
2. 从该切面出发的确定性 replay：重执行必须精确复现状态。

Storm core 两个前提都没有。它是 state-oblivious 的：容错协议只关心哪些输入事件已被完整处理、哪些该在超时后 replay，对算子状态一无所知。acker 跟踪的是消息树的完成，不是 accumulator 的值。于是 replay 发生时，状态要么随进程丢了（如 6.2），要么被同一条数据又改一遍；系统既观察不到偏差，也无从纠正。天花板因此是 at-least-once：消息不丢，状态账没人管。XOR acker 的机制细节见延伸阅读里的 Storm 官方文档。

Trident 的补丁是把状态搬进事务：micro-batch 处理，每批分配唯一且有序的事务 id，状态更新随批事务性提交；replay 的批带着已见过的 txid，直接忽略。这正是 5.4 节“严格两阶段提交 / batch 粒度”一族。状态账对了，代价是 micro-batch 的延迟与吞吐，以及另一套编程模型。

Flink 的改进是异步 consistent cut：checkpoint barrier 作为 marker 随数据流流动，不停流、不逐条提交，就切出“source offset 加全部算子状态”的 consistent cut。恢复时加载最近的 consistent cut 并从 source offset replay，确定性重执行精确复现状态。本例走一遍：从 CP#42 恢复，replay o6 后 W2 回到 (C=2, GMV=100)，再收 o7 得 (C=3, GMV=170)，与无故障执行分毫不差；对外的重复输出仍是 6.3 的问题，靠 sink 协议解决。

三家放在同一场崩溃下对比：

| 系统 | 故障前抓到什么 | 恢复动作 | 本例 W2 结局 |
| --- | --- | --- | --- |
| Storm core | 只有 tuple tree 的完成跟踪，无状态 snapshot | replay o6 一条 | (C=2, GMV=130)，永久错误 |
| Trident | micro-batch 边界上事务化提交的状态 | 已提交的批按 txid 去重，未提交的批整体 replay | (C=3, GMV=170)，正确 |
| Flink | CP#42：offset 加全部状态的 consistent cut | 加载切面，从 offset replay o6 | (C=3, GMV=170)，正确 |

所以“为什么现在的系统比 Storm 高级”的准确答案是：差别不在 replay 与否，三家都 replay；差别在故障之前抓了什么。Storm 什么都不抓，Trident 在每个 micro-batch 边界抓状态，Flink 异步抓整图 consistent cut。恢复粒度从逐条到逐批再到 consistent cut，状态从用户自理变成系统托管，正好接回 5.2 的归属表。

也要说一句公道话：如果计算本身无状态、逐条转发，Storm 的模型并不差，正常路径上只有定长空间的 ack 开销。“高级”二字仅针对有状态 exactly-once 成立。

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答“机器挂了之后，同一个窗口 accumulator 有几种活法”。前五种方案恢复出的 W2 一模一样，差别全在三个账户上：正常开销、恢复时间、输出正确性；Storm 不在这本账上，它连状态本身都救不回来。而 Storm、Trident、Flink 三家的差距不在 replay，在故障前抓了什么。</p>
</div>

值得注意的是，checkpoint 和 watermark 名字都像“屏障”，下一章把它们分开。

## 7. 收束：两条边界与第三本账

全篇的机制可以归拢成两条边界。**语义边界**：过去完整到哪里，由 Watermark 或 frontier 表达，决定何时输出、何时 revision、何时可以忘记历史。**恢复边界**：失败后回到哪里，由 ack、日志或 checkpoint 表达，决定输入位置、状态版本与输出提交怎样重新对齐。两条边界都会沿数据流传播，名字都像屏障，语义完全正交：

<figure class="fig-card fig-card--dense">
<svg class="fig-svg" viewBox="0 0 760 330" role="img" aria-label="Watermark 与 Checkpoint Barrier 是两条正交边界">
<defs>
<marker id="flow-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#57534e"/></marker>
</defs>
<text x="380" y="25" text-anchor="middle" class="t-title">同一条数据流上的两种边界</text>
<rect x="35" y="64" width="125" height="54" rx="10" fill="#f0fdfa" stroke="#0f766e"/><text x="97" y="86" text-anchor="middle" class="t-label">Source</text><text x="97" y="104" text-anchor="middle" class="t-sub">数据 + offset</text>
<rect x="315" y="64" width="125" height="54" rx="10" fill="#f5f3ff" stroke="#7c3aed"/><text x="377" y="86" text-anchor="middle" class="t-label">Stateful Operator</text><text x="377" y="104" text-anchor="middle" class="t-sub">窗口 / join 状态</text>
<rect x="600" y="64" width="125" height="54" rx="10" fill="#fff7ed" stroke="#b45309"/><text x="662" y="86" text-anchor="middle" class="t-label">Sink</text><text x="662" y="104" text-anchor="middle" class="t-sub">结果可见性</text>
<line x1="160" y1="91" x2="315" y2="91" stroke="#57534e" stroke-width="1.5" marker-end="url(#flow-arrow)"/>
<line x1="440" y1="91" x2="600" y2="91" stroke="#57534e" stroke-width="1.5" marker-end="url(#flow-arrow)"/>
<rect x="202" y="73" width="74" height="36" rx="18" fill="#0f766e"/><text x="239" y="95" text-anchor="middle" class="t-white">WM 12:10</text>
<rect x="486" y="73" width="76" height="36" rx="18" fill="#7c3aed"/><text x="524" y="95" text-anchor="middle" class="t-white">CP #42</text>
<path d="M239 118 L239 204" stroke="#0f766e" stroke-width="2" stroke-dasharray="5 4"/>
<path d="M524 118 L524 264" stroke="#7c3aed" stroke-width="2" stroke-dasharray="5 4"/>
<rect x="92" y="166" width="294" height="76" rx="12" fill="#f0fdfa" stroke="#99f6e4"/>
<text x="239" y="190" text-anchor="middle" class="t-title">Watermark：语义边界</text>
<text x="239" y="211" text-anchor="middle" class="t-sub">12:10 以前的 Event Time 是否还会改变？</text>
<text x="239" y="228" text-anchor="middle" class="t-sub">用于触发窗口、迟到判断、状态清理</text>
<rect x="377" y="226" width="294" height="76" rx="12" fill="#f5f3ff" stroke="#ddd6fe"/>
<text x="524" y="250" text-anchor="middle" class="t-title">Checkpoint：恢复边界</text>
<text x="524" y="271" text-anchor="middle" class="t-sub">失败后从哪些 source offset 与算子状态重启？</text>
<text x="524" y="288" text-anchor="middle" class="t-sub">用于一致快照、回放与 output commit</text>
</svg>
<figcaption class="fig-caption">Watermark 与 Checkpoint Barrier 都会沿图传播，但前者定义 Event-Time 完整性，后者定义故障恢复的一致切面。名字都像“屏障”，语义完全不同。</figcaption>
</figure>

这套正交性在三个系统里各自成立。Storm 的 XOR acker 以固定小空间追踪 tuple tree，分支没 ack 完就超时 replay：它建立恢复边界，不建立事件时间边界；用户放在 bolt 内存或外部数据库里的状态也不在它的 snapshot 范围内，idempotent 和事务要自己处理。Flink 的 barrier 在一致切面上保存 keyed/operator state，source 记录 Kafka offset：exactly-once on state 意味着每条记录对托管状态的最终影响与无故障执行一致，端到端则还要 source 可 replay、sink 支持事务或 idempotent。Timely/Naiad 的 frontier 能证明逻辑时间进度，却不能在进程消失后重建内存中的 arrangement 和 progress counts，所以 Naiad 另为有状态 vertex 定义了 CHECKPOINT/RESTORE 接口。frontier 回答“还能产生什么”，checkpoint 回答“已经积累的东西怎样恢复”。

用第五章的三种状态再读一遍这张图，每种状态各有自己的边界，两条正交边界因此看得更清楚：

| 状态 | 边界 | 机制 |
| --- | --- | --- |
| 输入进度 | 语义边界 | watermark / frontier |
| 工作状态加 offset | 恢复边界 | checkpoint / consistent cut |
| 输出进度 | 提交边界 | 输出提交协议（6.3） |

输入进度的边界收在语义边界里，工作状态 snapshot 与 offset 收在恢复边界里；输出进度的提交边界横跨两者，是输出提交问题的家。

第一节的六个问题，现在可以重新收拢：

| 问题 | 主要机制 | 本文位置 |
| --- | --- | --- |
| 无限数据怎样计算？ | 窗口、epoch、逻辑时间，把无界输入切成可推进的前缀 | 第 1、3 节 |
| 什么时候认为结果完整？ | Slack、low-watermark、pointstamp/frontier | 第 4 节 |
| 乱序和迟到怎么办？ | in-order 重排、out-of-order 进度界、allowed lateness | 第 3、4 节 |
| 输入变化后怎样更新结果？ | upsert、retract、revision 三路线、差分传播 | 第 4 节 |
| 哪些历史数据需要保存？ | 算子代数决定最小状态；synopsis、accumulator、join 索引、arrangement | 第 5 节 |
| 故障后怎样恢复？ | 上游备份、主动/被动备援、checkpoint + source replay、Storm 式逐条 replay（不管状态） | 第 6 节 |

只有语义边界没有恢复边界，能按时间触发却不能可靠恢复；只有恢复边界没有语义边界，能从故障中回来却永远不知道窗口何时该关。revision 机制则连接着两条边界之外的第三条事实：完成边界有时只是工程承诺，承诺被迟到数据或业务更正打破时，系统必须能撤回旧答案并传播新答案。

下一篇将沿着恢复边界继续：Checkpoint Barrier 如何在不停止数据流的情况下切出 consistent snapshot，exactly-once 为什么最终总会撞上输出提交，以及状态落入 RocksDB、LSM-tree 或远端存储后，恢复速度由什么决定。

## 延伸阅读

- Fragkoulis et al., *A Survey on the Evolution of Stream Processing Systems*（arXiv 2008.00842）：本文各节素材的主要来源与进一步阅读地图。§2.2 两代数据模型（本文第 3 节），§2.3 两代架构；§3.1/3.4 乱序成因与影响（本文第 2 节），§3.3 两种架构原型（本文第 3 节），§3.5.1 进度机制与 Figure 2（本文第 4 节），§3.5.2 循环查询的进度跟踪（本文 4.4），§3.5.3 revision 处理三路线（本文 4.6），§3.6 乱序管理的代际对比；§4.2 状态归属（本文 5.2），§4.5 分区与全局状态（本文 5.3），§4.4/4.6 持久化粒度与一致性（本文 5.4），§5.2 状态的代际对比；§5.1 处理语义（本文 6.1），§5.1.1 输出提交问题（本文 6.3），§5.2 高可用主被动与混合复制（本文 6.2），§5.3 容错的代际对比；§6 负载管理、弹性与重配置，本文未涉及。
- [Apache Storm: Guaranteeing Message Processing](https://storm.apache.org/releases/2.6.1/Guaranteeing-message-processing.html)：XOR acker 与 at-least-once（本文 4.6、6.2、6.4 的 Storm 机制细节以此为准）。
- [Apache Storm: Windowing Support](https://storm.apache.org/releases/2.6.2/Windowing.html)：Event Time、lag 与 Watermark。
- [Apache Flink: Streaming Analytics](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/streaming_analytics/)：Event Time、Watermark 与 lateness。
- [Apache Flink: Fault Tolerance](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/)：异步 barrier snapshot、恢复与端到端 exactly-once 条件。
- Murray et al., *Naiad: A Timely Dataflow System*：逻辑时间、pointstamp、frontier 与 checkpoint。
- McSherry et al., *Differential Dataflow*：`(data,time,diff)`、增量 join 与 arrangement。
- Hwang et al., *High-Availability Algorithms for Distributed Stream Processing*：主动/被动备援、上游备份与 amnesia 的恢复时间与开销建模。
