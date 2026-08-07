---
layout: post
title: "流计算基础（一）：从并行化到分布式计算 —— DAG 与 Loop"
date: 2026-07-27 10:00:00 +0800
categories: stream-processing
tags: [flink, timely-dataflow, 并行计算, 递归sql]
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
.post-content td { padding: 10px 12px; border-bottom: 1px solid #efe8db; }
.fig-card { background: #fffdf8; border: 1px solid #e8e0d4; border-radius: 14px; padding: 24px; margin: 32px 0; }
.fig-svg { width: 100%; height: auto; display: block; font-family: inherit; }
.fig-caption { margin-top: 12px; font-size: 0.85rem; font-weight: 500; color: #57534e; letter-spacing: 0.01em; }
.callout { padding: 16px 24px; border-radius: 0 10px 10px 0; background: #f6f1e7; margin: 24px 0; }
.callout p { margin: 0; }
.callout--insight { border-left: 4px solid #0f766e; }
.callout--caution { border-left: 4px solid #9a3412; }
.term { border-bottom: 1px dashed #a8a29e; font-weight: 600; }
.t-title { font-size: 13px; font-weight: 600; fill: #1c1917; }
.t-sub { font-size: 11px; fill: #57534e; }
.t-label { font-size: 12px; fill: #1c1917; }
.t-micro { font-size: 10px; fill: #a8a29e; }
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

本文是流计算基础系列的第一篇。读者多半写惯了数据库执行引擎，文章也就从数据库的执行模型讲起：火山模型（pull based）如何组织一次查询，push based 模型中 pipeline、DAG、dataflow 三个词各管什么事（§0）。在这个底座之上，再讨论并行计算中最基本的问题：一项计算能够被并行到什么程度，以及决定这一上限的因素是什么。

执行模型交代清楚之后，正文从计算之间的依赖关系出发。把一项计算画成 DAG 时，每个节点代表一段可以由线程独立完成的工作，例如读取一批数据、执行一次 join 或合并一组中间结果；每条有向边代表数据依赖，表示下游节点必须等上游产生了对应数据，才具备执行条件。没有依赖边相连的节点可以同时运行，最长的依赖链则决定了增加处理器之后，执行时间还能缩短多少。

现代 DAG 系统通常不要求程序员逐个指定线程的调用次序。程序描述数据要经过哪些变换、按什么键被分区、结果要流向哪些算子，编译器和运行时据此生成执行图，把算子实例部署到 worker，再让每个 worker 从到达自己的数据中选择可执行的工作。换句话说，图的节点编码“收到这类数据时做什么”，边编码“产生的数据送到哪里”。对于不包含循环的计算，这张图既给出了执行次序，也揭示了它与函数式表达式之间的对应关系。

然而，许多计算无法在一张静态的 DAG 中完成。递归查询、图遍历和迭代算法都需要把上一轮的结果送入下一轮，直到满足终止条件。MPI、Pregel 和 Flink 通常以同步轮次组织这类计算；Timely Dataflow 则把逻辑时间附着在数据上，使不同轮次的工作能够同时推进。算子一边处理正在到达的消息，一边通过进度消息持续更新 frontier：数据计算与“未来还可能出现什么时间”的判断始终同时发生。两种方法的差异，实质上是两种表达计算进度的方式。

一次并行计算通常处理一批有限的数据，计算完成，任务也随之结束。如果新的数据持续到来，同一项计算就要反复进行。系统可以用 epoch 标记数据所属的逻辑阶段；前一个 epoch 留下的结果，如果还要参与后一个 epoch 的计算，就形成了状态。状态如何保存、恢复并保持一致，将在后续文章中讨论。

下面先从数据库读者最熟悉的执行模型讲起。

## 0. 从火山模型到 dataflow 模型

这一章交代两套执行模型的关系：火山模型怎么组织一次查询，输入无界之后哪里失效，以及 pipeline、DAG、dataflow 三个词各自的分工。

### 0.1 火山模型：一次查询的一生

大多数数据库的执行引擎都是火山模型（Volcano model）：执行计划是一棵算子树，每个算子对外只暴露三个接口——`open()`、`next()`、`close()`。根节点要一条数据，就调用子算子的 `next()`，子算子再向自己的子算子要，一路向下；数据沿调用栈一路向上返回。控制流向下，数据流向上，一次 `next()` 搬运一条元组。这个模型从 System R 的 iterator model 定型，因 Graefe 的 Volcano 系统得名。

拉模型（pull）当年胜出，理由都很实际：

1. 流控由调用关系自然带出——消费者不调用 `next()`，数据就不动，生产者永远不会跑赢消费者；
2. 组合性——每个算子是黑盒，优化器可以自由替换实现；
3. 阻塞算子有现成的表达方式——sort、hash join 的 build 侧只需要在第一次被调用时把输入吃光；
4. 取消也很直接——停止调用 `next()` 就是停止执行；
5. 调度简单——一条流水线一个线程，元组在调用栈里流动，算子之间不需要缓冲队列。

注意第 3 条里的分界。执行引擎的经典视角是把计划树沿阻塞算子（pipeline breaker）切开：scan、filter、project 这类算子随产随消，它们组成的极大链就是一条 pipeline，内部不落任何中间结果；碰到 sort、聚合、hash join 的 build 侧，数据必须物化，成为下一条 pipeline 的输入。**Volcano 的 pipeline 是被 breaker 切碎的有限线段，线段的寿命等于查询的寿命。**

<figure class="fig-card fig-card--dense">
<svg class="fig-svg" viewBox="0 0 800 440" role="img" aria-label="左：算子树，HashBuild 与 Sort 标为 breaker，切口画在 breaker 处；右：三条 pipeline 各占一个线程，哈希表与有序数据经物化点交接，寿命与查询相同">
<defs>
<marker id="fig1-idle" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#a8a29e"/></marker>
<marker id="fig1-orange" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#9a3412"/></marker>
</defs>
<text x="195" y="28" text-anchor="middle" class="t-title" fill="#9a3412">计划树：breaker 在哪里，切分就在哪里</text>
<text x="195" y="46" text-anchor="middle" class="t-sub">数据自下而上流动，碰到 breaker 就物化</text>
<text x="592" y="28" text-anchor="middle" class="t-title" fill="#9a3412">执行视图：每条 pipeline 一个线程</text>
<text x="592" y="46" text-anchor="middle" class="t-sub">物化点是流水线之间的交接</text>
<line x1="390" y1="20" x2="390" y2="420" stroke="#e8e0d4" stroke-width="1" stroke-dasharray="2 4"/>
<g stroke="#a8a29e" stroke-width="1.5" fill="none">
<line x1="135" y1="336" x2="135" y2="306" marker-end="url(#fig1-idle)"/>
<line x1="135" y1="268" x2="168" y2="238" marker-end="url(#fig1-idle)"/>
<line x1="285" y1="336" x2="285" y2="306" marker-end="url(#fig1-idle)"/>
<line x1="285" y1="268" x2="246" y2="237" marker-end="url(#fig1-idle)"/>
<line x1="195" y1="200" x2="195" y2="170" marker-end="url(#fig1-idle)"/>
<line x1="195" y1="132" x2="195" y2="102" marker-end="url(#fig1-idle)"/>
</g>
<g>
<rect x="147" y="64" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="195" y="85" text-anchor="middle" class="t-label">汇总</text>
<rect x="147" y="132" width="96" height="34" rx="9" fill="#ffedd5" stroke="#9a3412" stroke-width="1.5"/><text x="195" y="153" text-anchor="middle" class="t-label">Sort</text>
<rect x="147" y="200" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="195" y="221" text-anchor="middle" class="t-label">HashJoin</text>
<rect x="87" y="268" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="135" y="289" text-anchor="middle" class="t-label">Filter</text>
<rect x="237" y="268" width="96" height="34" rx="9" fill="#ffedd5" stroke="#9a3412" stroke-width="1.5"/><text x="285" y="289" text-anchor="middle" class="t-label">HashBuild</text>
<rect x="87" y="336" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="135" y="357" text-anchor="middle" class="t-label">Scan·orders</text>
<rect x="237" y="336" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="285" y="357" text-anchor="middle" class="t-label">Scan·cust</text>
</g>
<g stroke="#9a3412" stroke-width="2" stroke-dasharray="7 5" fill="none">
<line x1="135" y1="115" x2="255" y2="115"/>
<line x1="256" y1="264" x2="277" y2="238"/>
</g>
<text x="263" y="119" class="t-micro" fill="#9a3412">物化：有序数据</text>
<text x="290" y="242" class="t-micro" fill="#9a3412">物化：哈希表</text>
<rect x="30" y="398" width="16" height="16" rx="4" fill="#ffedd5" stroke="#9a3412" stroke-width="1.2"/>
<text x="54" y="411" class="t-sub">pipeline breaker：数据在此物化，流水线在此断开</text>
<g>
<rect x="405" y="64" width="375" height="64" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="417" y="82" class="t-micro" fill="#9a3412" font-weight="700">P1 · 线程 1</text>
<rect x="417" y="90" width="82" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="458" y="107" text-anchor="middle" class="t-label">Scan·cust</text>
<line x1="499" y1="103" x2="509" y2="103" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="511" y="90" width="96" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="559" y="107" text-anchor="middle" class="t-label">构建哈希表</text>
<line x1="607" y1="103" x2="617" y2="103" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="619" y="90" width="124" height="26" rx="7" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/><text x="681" y="107" text-anchor="middle" class="t-label">哈希表 · 物化</text>
<rect x="405" y="184" width="375" height="64" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="417" y="202" class="t-micro" fill="#9a3412" font-weight="700">P2 · 线程 2</text>
<rect x="417" y="210" width="88" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="461" y="227" text-anchor="middle" class="t-label">Scan·orders</text>
<line x1="505" y1="223" x2="515" y2="223" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="517" y="210" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="549" y="227" text-anchor="middle" class="t-label">Filter</text>
<line x1="581" y1="223" x2="591" y2="223" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="593" y="210" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="625" y="227" text-anchor="middle" class="t-label">⋈ probe</text>
<line x1="657" y1="223" x2="667" y2="223" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="669" y="210" width="98" height="26" rx="7" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/><text x="718" y="227" text-anchor="middle" class="t-label">有序数据·物化</text>
<rect x="405" y="304" width="375" height="64" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="417" y="322" class="t-micro" fill="#9a3412" font-weight="700">P3 · 线程 3</text>
<rect x="417" y="330" width="104" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="469" y="347" text-anchor="middle" class="t-label">读回有序数据</text>
<line x1="521" y1="343" x2="531" y2="343" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="533" y="330" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="565" y="347" text-anchor="middle" class="t-label">汇总</text>
<line x1="597" y1="343" x2="607" y2="343" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="609" y="330" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="641" y="347" text-anchor="middle" class="t-label">输出</text>
</g>
<g stroke="#9a3412" stroke-width="1.8" stroke-dasharray="6 4" fill="none">
<line x1="681" y1="120" x2="625" y2="209" marker-end="url(#fig1-orange)"/>
<path d="M 718 240 L 718 274 L 469 274 L 469 329" marker-end="url(#fig1-orange)"/>
</g>
<text x="694" y="160" class="t-micro" fill="#9a3412">probe 前必须先建好</text>
<text x="594" y="268" text-anchor="middle" class="t-micro" fill="#9a3412">交给 P3 扫描读回</text>
<g stroke="#9a3412" stroke-width="1.5" fill="none">
<line x1="405" y1="388" x2="780" y2="388"/>
<line x1="405" y1="382" x2="405" y2="394"/>
<line x1="780" y1="382" x2="780" y2="394"/>
</g>
<text x="592" y="410" text-anchor="middle" class="t-sub">三条 pipeline 与查询同生共死：查询开始才建图，查询结束，计划与线程全部销毁</text>
</svg>
<figcaption class="fig-caption">左：一棵执行计划树，橙色节点是 pipeline breaker——hash join 的 build 侧与 Sort，数据必须在这里物化才能继续向上。右：沿两个 breaker 切开，得到三条 pipeline，每条是一个独立线程；物化点是流水线之间的交接。三者的寿命完全相同：查询开始时建图，查询结束时销毁。</figcaption>
</figure>

并行化也在同一框架里完成：Graefe 把 shuffle 包装成一个普通算子（Exchange）插进树里，树内走流水线，跨 exchange 换线程、换机器。今天所有 MPP 引擎的并行执行——包括 OceanBase PX 把计划切成 DFO 分发——本质都是"火山模型 + exchange"。

### 0.2 输入无界：火山模型里原本自然成立的三件事变成了问题

此后三十年，数据库阵营对这套模型的改造都没有超出"一次查询"的前提：per-tuple 调用开销大，就一次返回一批——向量化与 morsel-driven 执行；pipeline 内解释执行慢，就把整条 pipeline 编译成 push 风格的机器码。这些改造压的是开销，不是前提：**输入有限，查询有始有终。**

流处理是另一个谱系。思想源头更早：Kahn 1974 年提出的进程网络——常驻的处理单元用通道相连，数据到达即触发；1970 年代的数据流机器（dataflow architecture）沿同一思路。这条线与数据库几十年互不来往，直到 2002 年前后连续查询出现：Aurora、STREAM、TelegraphCQ 不约而同选择了 push。系统代际的完整谱系在第二篇，这里只看换轨的原因。

选择 push 的原因很直接。输入无界，根节点没有"查询结束"这一刻：拉模型的线程会永远阻塞在 source 的空等上；push 让线程在没数据时睡觉，数据到达时被唤醒。输入无界之后，火山模型里原本自然成立的三件事，都变成了需要专门处理的问题：

| 火山模型里原本自然成立的事 | 无界输入下变成的问题 | 流系统的应对 |
|---|---|---|
| `close()` 总会到来 | 永不发生，算子必须跨调用保存中间结果 | 状态与恢复（第二、三篇） |
| 阻塞算子终将吃完输入 | sort、全量聚合永远吃不完 | 窗口与时间范围 |
| 输入有限，"算完了"是自然事实 | 无法宣布某段时间的答案已经齐了 | 进度消息：标点消息（punctuation）→ watermark / frontier |

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 760 440" role="img" aria-label="上：火山模型的生命周期 open、next 若干次、close，随后是查询结束的明确终点，终点之后计划销毁；下：流系统的生命周期 open 之后记录持续到达，close 以虚线框标注永不发生，下方三个框为状态、窗口、进度三笔债">
<defs>
<marker id="fig2-gray" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#a8a29e"/></marker>
</defs>
<text x="380" y="28" text-anchor="middle" class="t-title">同一段算子代码，两种生命周期</text>
<text x="380" y="46" text-anchor="middle" class="t-sub">有限输入 vs 无界输入：差别在终点是否已知</text>
<text x="20" y="92" class="t-label" fill="#9a3412" font-weight="600">火山模型 · 拉</text>
<text x="20" y="110" class="t-sub">输入有限，执行前已知终点</text>
<line x1="120" y1="140" x2="660" y2="140" stroke="#e8e0d4" stroke-width="1"/>
<rect x="128" y="126" width="62" height="28" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4"/><text x="159" y="144" text-anchor="middle" class="t-label">open()</text>
<g fill="#9a3412">
<circle cx="226" cy="140" r="3"/><circle cx="254" cy="140" r="3"/><circle cx="282" cy="140" r="3"/><circle cx="310" cy="140" r="3"/><circle cx="338" cy="140" r="3"/><circle cx="366" cy="140" r="3"/>
</g>
<text x="296" y="122" text-anchor="middle" class="t-micro" fill="#9a3412" font-weight="700">next() × N</text>
<rect x="400" y="126" width="62" height="28" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4"/><text x="431" y="144" text-anchor="middle" class="t-label">close()</text>
<line x1="500" y1="108" x2="500" y2="172" stroke="#9a3412" stroke-width="2.2"/>
<text x="500" y="100" text-anchor="middle" class="t-micro" fill="#9a3412" font-weight="700">查询结束</text>
<rect x="512" y="112" width="200" height="56" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="612" y="134" text-anchor="middle" class="t-micro">计划销毁 · 资源释放</text>
<text x="612" y="152" text-anchor="middle" class="t-micro" fill="#57534e">算子不留下任何东西</text>
<text x="20" y="238" class="t-label" fill="#0f766e" font-weight="600">流系统 · 推</text>
<text x="20" y="256" class="t-sub">输入无界，没有终点</text>
<line x1="120" y1="282" x2="728" y2="282" stroke="#e8e0d4" stroke-width="1" marker-end="url(#fig2-gray)"/>
<rect x="128" y="268" width="62" height="28" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.4"/><text x="159" y="286" text-anchor="middle" class="t-label">open()</text>
<g fill="#0f766e">
<circle cx="226" cy="282" r="3"/><circle cx="252" cy="282" r="3"/><circle cx="278" cy="282" r="3"/><circle cx="304" cy="282" r="3"/><circle cx="330" cy="282" r="3"/><circle cx="356" cy="282" r="3"/><circle cx="382" cy="282" r="3"/><circle cx="408" cy="282" r="3"/><circle cx="434" cy="282" r="3"/>
</g>
<text x="476" y="287" text-anchor="middle" class="t-label" fill="#0f766e">· · ·</text>
<text x="330" y="264" text-anchor="middle" class="t-micro" fill="#0f766e">记录持续到达，永不停</text>
<rect x="636" y="268" width="84" height="28" rx="8" fill="#fffdf8" stroke="#0f766e" stroke-width="1.5" stroke-dasharray="5 4"/><text x="678" y="286" text-anchor="middle" class="t-label" fill="#0f766e">close()</text>
<text x="678" y="258" text-anchor="middle" class="t-micro" fill="#9a3412" font-weight="700">永不发生</text>
<g stroke="#0f766e" stroke-width="1.2" stroke-dasharray="3 3" fill="none">
<line x1="250" y1="290" x2="250" y2="328"/>
<line x1="470" y1="290" x2="470" y2="328"/>
<line x1="665" y1="290" x2="665" y2="328"/>
</g>
<rect x="150" y="330" width="200" height="64" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.3"/>
<text x="162" y="352" class="t-label" fill="#0f766e" font-weight="600">状态</text>
<text x="162" y="370" class="t-micro">close() 从不发生</text>
<text x="162" y="384" class="t-micro">中间结果必须跨调用存活（第二、三篇）</text>
<rect x="370" y="330" width="200" height="64" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.3"/>
<text x="382" y="352" class="t-label" fill="#0f766e" font-weight="600">窗口</text>
<text x="382" y="370" class="t-micro">sort / 全量聚合</text>
<text x="382" y="384" class="t-micro">永远等不到全部输入</text>
<rect x="590" y="330" width="150" height="64" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.3"/>
<text x="602" y="352" class="t-label" fill="#0f766e" font-weight="600">进度</text>
<text x="602" y="370" class="t-micro">完成必须被制造</text>
<text x="602" y="384" class="t-micro">punctuation → watermark</text>
<text x="20" y="424" class="t-micro">时间 →</text>
</svg>
<figcaption class="fig-caption">同样的算子，两种生命周期。上（火山）：输入有限，open → next()×N → close 有明确终点；查询结束后计划销毁，算子不留下任何东西。下（流系统）：输入不停，close() 永不发生——这一个事实生出三笔债：状态（中间结果必须跨调用存活）、窗口（阻塞算子永远等不到全部输入）、进度消息（完成必须由 punctuation / watermark 制造出来）。</figcaption>
</figure>

第三笔债最容易被低估。完成不再自动发生，系统就必须自己制造完成：标点消息是一条控制消息，声明"某时间之前的数据不会再来了"——它是第三篇 watermark 与本篇 §4.2 frontier 的直系祖先。

一句话收束：**从火山到数据流，表面上是拉与推的方向反转，实质是"计算"的生命周期从一次查询变成常驻服务，"进度"从隐含前提变成必须显式制造的一等公民。** 后面的所有概念——状态、窗口、watermark、frontier——都在为这两件事还债。

### 0.3 三个词各管一件事：DAG、Pipeline、Dataflow

一串算子连起来的计算有三个名字，论文和文档经常混用；数据库背景的读者更容易混——引擎里说 pipeline，指执行计划的一段，流系统里说 pipeline，又指整个引擎。这里把分工说清：每个词只回答一个问题。

**DAG 回答"谁在等谁"。** 把一项计算里的机器、线程、时序全部拿走，只留下"这段计算要读到什么数据才能做"：节点是计算，有向边是数据依赖，不出现环，就是一张 DAG。§1 那棵八个数的求和树是 DAG，Hillis-Steele 扫描是 DAG，一次 SQL 的执行计划也是 DAG。DAG 描述计算的形状，不涉及怎么执行；本篇 §1–§4 研究的正是这个形状允许什么、不允许什么。

**Pipeline 回答"数据流到哪里要停"。** 在同一张 DAG 上沿某些边界画封闭曲线：火山模型里，刀落在阻塞算子上，刀口以内数据随产随消，跨刀口先物化再交给下一段；流系统里，刀落在跨网络、需要物化或带 feedback 的位置，刀口以内的算子熔进同一个线程——Flink 的 operator chain 与 task 边界就是这条刀口，这个定义来自 Stratosphere 系统（Hueske et al., 2012，即 Flink 的前身）。**Pipeline 不是另一种图，而是同一张 DAG 的一种切分方式：DAG 画依赖，pipeline 画边界。**

**Dataflow 回答"谁指挥谁干活"。** 火山模型的回答是调用栈：父算子调用子算子的 `next()`，执行次序写在调用关系里；dataflow 的回答是数据到达：一条记录流到哪条边，负责那条边的算子就开工。这是执行语义，不绑定形状——纯 DAG 可以 dataflow 驱动，带回边的图也可以，后者只是要多一层逻辑时间（§4）。术语源流交代一句：dataflow 一词来自 1970 年代的数据流机器与 Kahn 进程网络，今天的 Naiad、Flink、Timely 都算这一支。这些系统的执行图形状通常也是 DAG，所以 dataflow graph 常与 DAG 混写；真正的差别只有一处：**dataflow 的图允许回边，DAG 不允许**——这正是 §3 和 §4 的故事。

把三个词叠到同一套系统上，正好三层：

| 层 | 回答的问题 | 火山时代 | 流时代 |
|---|---|---|---|
| 形状 | 谁依赖谁 | 执行计划树；树内不能有环，递归只能靠树外循环（0.4 展开） | 数据流图；允许 feedback 回边（§4） |
| 切分 | 数据在哪里落地 | breaker 切出 pipeline，一段一个线程 | exchange 切出 pipeline，段内 fuse 成 task |
| 驱动 | 下一步做什么 | `next()` 调用栈 | 记录到达，算子即开工 |

**DAG 画计算的依赖，pipeline 画执行的边界，dataflow 把执行的主动权交给数据。**

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答的问题是"同一个系统为什么有三个名字"。判断标准只有一条：问形状的去找 DAG，问边界的去找 pipeline，问调度的去找 dataflow。第一代系统（Aurora、STREAM）换掉的是第三层，并第一次把进度做成消息；第二代系统进一步改了第一层，把回边和逻辑时间收进图里。</p>
</div>

### 0.4 无限机器时，执行语义就是算法本身

选 dataflow 做底座，还有一个比"输入无界"更根本的理由：**push 模型可以表达任何计算。**

先看表达力。纯函数是算子，顺序是边，分支是"数据按内容走哪条边"——控制流被改写成数据流；循环与递归是 feedback 回边，终止条件退化成进度判定（§3）。§2.1 说过 DAG 与纯函数表达式同构，§4.2.5 又说 `iterate` 把递归函数收编进来：纯函数、组合、递归，三样拼齐恰好就是可计算函数。这不是新结论。这个模型的早期形式化是 Kahn 1974 年的进程网络：确定性处理单元用无界 FIFO 通道相连，进程本身是任意顺序程序，网络允许反馈。这样的网络可以模拟通用图灵机，因而是图灵完备的——它会不会终止，本身就不可判定。通用性的代价只有一句：图里不再有人替你看全局进度。

再看执行。§1 的 Brent 定理说，机器趋于无限时，执行时间里只剩下关键路径。这个极限下，调度完全由依赖关系决定：每段计算的输入一就绪就执行，如此而已。push 语义正是把这个极限情形写成常驻规则——记录流到哪条边，负责那条边的算子立刻开工。规则不预设机器数量：一台机器上消息排队，一万台机器上消息扇出，对确定性算子，变的只是速度，不是结果。**每一次有限规模的执行，都只是这份无限并行计划被节流后的投影。**

所以准确的说法不是"pull 算不了"。火山模型加一个树外 driver，同样能写出任何算法。所谓树外 driver，是算子树外面的命令式循环：火山树不能有环——子节点若沿树指回父节点，`next()` 调用会无限嵌套——所以"把本轮结果送回下一轮"必须放在树外做：

```text
work = 执行(锚成员)，物化为工作表            # 第 0 轮
while work ≠ ∅:
    work = 重新打开(递归子树, 输入 = work)   # 再跑一轮
    work = work − 已知集合                   # 终止判据
    已知集合 = 已知集合 ∪ work
```

这正是 §4.4.2 里 OceanBase 执行 `WITH RECURSIVE` 的循环：每一轮内部可以 PX 并行，轮与轮之间串行。算子树本身是有界计算，加上"反复执行、检查结果、决定终止"的循环，就得到一般迭代，也就得到了通用计算能力——pull 不是算不了，它是用命令式代码表达循环。代价是这层循环成了胶水代码：优化器看不见，轮间无法并行，递归查询、相关子查询、迭代算法各要手写一个专属 driver。而 dataflow 里 feedback 就是图内一条数据边，轮次靠时间戳区分，终止靠 frontier 判定——一套机制覆盖所有循环。

**pull 把调度写进程序，push 把调度留给运行时。** 前者的运行时沿调用栈走程序；后者的运行时做规划——追踪哪些计算已经就绪，哪些时间已经关闭：一个是 §4.5 的火山 driver，一个是 §4.2 的 frontier。

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答的问题是"为什么 dataflow 是通用底座"。表达力：带回边的图图灵完备；可实现性：push 语义不假设机器数量，无限并行的极限恰好是依赖本身。通用的账在 §3 和 §4 付——回边要自己判定进度，无界输入要自己制造完成。</p>
</div>

从这里开始，视角换到流系统本身：先问这张常驻的图能并行到什么程度（§1–§2），再问它装不下的计算需要什么（§3–§4）。

## 1. 关键路径：依赖关系给并行计算划定的极限

许多并行算法都遵循分而治之的思路：先把原问题分解成若干可以独立处理的子问题，再把各个子问题的结果合并起来。分解决定哪些工作可以同时进行，合并则建立了子问题之间的先后关系。这些关系共同构成了计算的依赖结构。

拿 8 个数 `[3, 1, 4, 1, 5, 9, 2, 6]` 算两笔账。

第一笔，求和。哪两个数先加都不影响结果，8 个数可以组织成一棵 3 层的树：4 对同时加，2 个中间和同时加，最后一次收尾。7 次加法，3 层算完，终点 31。

第二笔，前缀和：第 i 个位置要输出前 i 个数之和。第 5 个位置的答案必须包含第 4 个位置的答案——每一步都在等上一步。依赖连成一条 7 层的链，还是 7 次加法，但多少台机器都只能一层一层往下走，终点同样是 31。

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 720 350" role="img" aria-label="求和组成深度3的树，前缀和连成深度7的链">
<defs>
<marker id="fa-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
<marker id="fa-orange" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#9a3412"/></marker>
</defs>
<text x="170" y="28" text-anchor="middle" class="t-title" fill="#0f766e">求和 · 谁也不等谁</text>
<text x="170" y="46" text-anchor="middle" class="t-sub">树：深度 3，7 次加法</text>
<text x="550" y="28" text-anchor="middle" class="t-title" fill="#9a3412">前缀和 · 步步等上一步</text>
<text x="550" y="46" text-anchor="middle" class="t-sub">链：深度 7，7 次加法</text>
<line x1="360" y1="16" x2="360" y2="334" stroke="#e8e0d4" stroke-width="1" stroke-dasharray="2 4"/>
<g stroke="#0f766e" stroke-width="1.6" fill="none">
<line x1="25" y1="98" x2="45" y2="142" marker-end="url(#fa-teal)"/><line x1="65" y1="98" x2="45" y2="142" marker-end="url(#fa-teal)"/>
<line x1="105" y1="98" x2="125" y2="142" marker-end="url(#fa-teal)"/><line x1="145" y1="98" x2="125" y2="142" marker-end="url(#fa-teal)"/>
<line x1="185" y1="98" x2="205" y2="142" marker-end="url(#fa-teal)"/><line x1="225" y1="98" x2="205" y2="142" marker-end="url(#fa-teal)"/>
<line x1="265" y1="98" x2="285" y2="142" marker-end="url(#fa-teal)"/><line x1="305" y1="98" x2="285" y2="142" marker-end="url(#fa-teal)"/>
<line x1="45" y1="170" x2="85" y2="214" marker-end="url(#fa-teal)"/><line x1="125" y1="170" x2="85" y2="214" marker-end="url(#fa-teal)"/>
<line x1="205" y1="170" x2="245" y2="214" marker-end="url(#fa-teal)"/><line x1="285" y1="170" x2="245" y2="214" marker-end="url(#fa-teal)"/>
<line x1="85" y1="242" x2="165" y2="286" marker-end="url(#fa-teal)"/><line x1="245" y1="242" x2="165" y2="286" marker-end="url(#fa-teal)"/>
</g>
<g>
<circle cx="25" cy="84" r="14" fill="#ffffff" stroke="#0f766e" stroke-width="1.5"/><text x="25" y="88" text-anchor="middle" class="t-label">3</text>
<circle cx="65" cy="84" r="14" fill="#ffffff" stroke="#0f766e" stroke-width="1.5"/><text x="65" y="88" text-anchor="middle" class="t-label">1</text>
<circle cx="105" cy="84" r="14" fill="#ffffff" stroke="#0f766e" stroke-width="1.5"/><text x="105" y="88" text-anchor="middle" class="t-label">4</text>
<circle cx="145" cy="84" r="14" fill="#ffffff" stroke="#0f766e" stroke-width="1.5"/><text x="145" y="88" text-anchor="middle" class="t-label">1</text>
<circle cx="185" cy="84" r="14" fill="#ffffff" stroke="#0f766e" stroke-width="1.5"/><text x="185" y="88" text-anchor="middle" class="t-label">5</text>
<circle cx="225" cy="84" r="14" fill="#ffffff" stroke="#0f766e" stroke-width="1.5"/><text x="225" y="88" text-anchor="middle" class="t-label">9</text>
<circle cx="265" cy="84" r="14" fill="#ffffff" stroke="#0f766e" stroke-width="1.5"/><text x="265" y="88" text-anchor="middle" class="t-label">2</text>
<circle cx="305" cy="84" r="14" fill="#ffffff" stroke="#0f766e" stroke-width="1.5"/><text x="305" y="88" text-anchor="middle" class="t-label">6</text>
<circle cx="45" cy="156" r="14" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="45" y="160" text-anchor="middle" class="t-label">4</text>
<circle cx="125" cy="156" r="14" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="125" y="160" text-anchor="middle" class="t-label">5</text>
<circle cx="205" cy="156" r="14" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="205" y="160" text-anchor="middle" class="t-label">14</text>
<circle cx="285" cy="156" r="14" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="285" y="160" text-anchor="middle" class="t-label">8</text>
<circle cx="85" cy="228" r="14" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="85" y="232" text-anchor="middle" class="t-label">9</text>
<circle cx="245" cy="228" r="14" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="245" y="232" text-anchor="middle" class="t-label">22</text>
<circle cx="165" cy="300" r="16" fill="#0f766e"/><text x="165" y="304" text-anchor="middle" class="t-white">31</text>
</g>
<g stroke="#9a3412" stroke-width="1.8" fill="none">
<line x1="412" y1="97" x2="422" y2="104" marker-end="url(#fa-orange)"/>
<line x1="450" y1="130" x2="460" y2="137" marker-end="url(#fa-orange)"/>
<line x1="488" y1="163" x2="498" y2="170" marker-end="url(#fa-orange)"/>
<line x1="526" y1="196" x2="536" y2="203" marker-end="url(#fa-orange)"/>
<line x1="564" y1="229" x2="574" y2="236" marker-end="url(#fa-orange)"/>
<line x1="602" y1="262" x2="612" y2="269" marker-end="url(#fa-orange)"/>
<line x1="640" y1="295" x2="650" y2="302" marker-end="url(#fa-orange)"/>
</g>
<g>
<circle cx="398" cy="84" r="14" fill="#ffffff" stroke="#9a3412" stroke-width="1.5"/><text x="398" y="88" text-anchor="middle" class="t-label">3</text>
<circle cx="436" cy="117" r="14" fill="#ffffff" stroke="#9a3412" stroke-width="1.5"/><text x="436" y="121" text-anchor="middle" class="t-label">4</text>
<circle cx="474" cy="150" r="14" fill="#ffffff" stroke="#9a3412" stroke-width="1.5"/><text x="474" y="154" text-anchor="middle" class="t-label">8</text>
<circle cx="512" cy="183" r="14" fill="#ffffff" stroke="#9a3412" stroke-width="1.5"/><text x="512" y="187" text-anchor="middle" class="t-label">9</text>
<circle cx="550" cy="216" r="14" fill="#ffffff" stroke="#9a3412" stroke-width="1.5"/><text x="550" y="220" text-anchor="middle" class="t-label">14</text>
<circle cx="588" cy="249" r="14" fill="#ffffff" stroke="#9a3412" stroke-width="1.5"/><text x="588" y="253" text-anchor="middle" class="t-label">23</text>
<circle cx="626" cy="282" r="14" fill="#ffffff" stroke="#9a3412" stroke-width="1.5"/><text x="626" y="286" text-anchor="middle" class="t-label">25</text>
<circle cx="664" cy="315" r="14" fill="#9a3412"/><text x="664" y="319" text-anchor="middle" class="t-white">31</text>
</g>
<g class="t-micro">
<text x="416" y="80">+3</text>
<text x="454" y="113">+1</text>
<text x="492" y="146">+4</text>
<text x="530" y="179">+1</text>
<text x="568" y="212">+5</text>
<text x="606" y="245">+9</text>
<text x="644" y="278">+2</text>
<text x="682" y="311">+6</text>
</g>
<text x="170" y="340" text-anchor="middle" class="t-sub">输入自上而下合并，3 层得到结果</text>
<text x="550" y="345" text-anchor="middle" class="t-sub">7 层，一层都不能跳</text>
</svg>
<figcaption class="fig-caption">同样 8 个数、同样 7 次加法。左边没人等谁，3 层算完；右边每一步都在等上一步，7 层一层不少。机器再多，也快不过图里最长那条链。</figcaption>
</figure>

同样的数据，同样的计算量，快慢差了两倍多——差距不在机器，在图的形状。这里有两个名词值得记住：

- **工作量（work）**：全部加法的次数，7 次。它等于只用一台机器串行跑完所需的时间（T₁）——无论投入多少机器，这笔总账不会变少，只是被分摊到同一时刻并行支付。
- **关键路径（critical path，也叫 span）**：图里最长的那条依赖链。它等于有无限多机器时的最短完成时间（T∞）——机器加得再多，也快不过这条链。理论说法是 Brent 定理：p 台机器的执行时间 `T(p) ≤ T₁/p + T∞`，机器趋于无限时，剩下的只有 T∞，也就是关键路径的长度。

一句话：**机器决定图的宽度，依赖决定图的长度；并行时间的下限，写在长度里。**

朴素前缀和只维护一组不断向后传递的结果，因此中间变量很少，全部计算却集中在一条依赖链上。若为每一轮保留一组中间结果，情况便会改变：本轮只读取上一轮的数据，本轮各位置之间没有数据依赖，可以同时计算。

Hillis-Steele 扫描采用的正是这一方法。第一轮，每个位置合并左侧相距 1 个位置的结果；第二轮把距离扩大到 2；第三轮再扩大到 4。每一轮都会新增一层中间变量，同时使每个结果覆盖的输入区间扩大一倍。对于 8 个数，原来长度为 7 的依赖链因此变成 3 轮并行计算。

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 720 410" role="img" aria-label="Hillis-Steele 扫描的三轮数据依赖：每一轮都只读取上一轮结果，同一轮各位置可以并行计算">
<defs>
<marker id="fb-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
<marker id="fb-gray" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#a8a29e"/></marker>
</defs>
<text x="360" y="24" text-anchor="middle" class="t-title">Hillis-Steele 扫描：增加中间变量，缩短依赖链</text>
<text x="360" y="42" text-anchor="middle" class="t-sub">每一行是一组独立变量；箭头只跨越相邻两轮，不在同一轮内部连接</text>
<g class="t-sub" text-anchor="end">
<text x="146" y="86">输入 x⁰</text>
<text x="146" y="168">第 1 轮 x¹</text>
<text x="146" y="250">第 2 轮 x²</text>
<text x="146" y="332">第 3 轮 x³</text>
</g>
<g stroke="#a8a29e" stroke-width="1.15" fill="none" opacity="0.62">
<path d="M188 100 L188 146"/><path d="M232 100 L232 146"/><path d="M276 100 L276 146"/><path d="M320 100 L320 146"/><path d="M364 100 L364 146"/><path d="M408 100 L408 146"/><path d="M452 100 L452 146"/><path d="M496 100 L496 146"/>
<path d="M188 182 L188 228"/><path d="M232 182 L232 228"/><path d="M276 182 L276 228"/><path d="M320 182 L320 228"/><path d="M364 182 L364 228"/><path d="M408 182 L408 228"/><path d="M452 182 L452 228"/><path d="M496 182 L496 228"/>
<path d="M188 264 L188 310"/><path d="M232 264 L232 310"/><path d="M276 264 L276 310"/><path d="M320 264 L320 310"/><path d="M364 264 L364 310"/><path d="M408 264 L408 310"/><path d="M452 264 L452 310"/><path d="M496 264 L496 310"/>
</g>
<g stroke="#0f766e" stroke-width="1.7" fill="none">
<path d="M188 100 L232 146" marker-end="url(#fb-teal)"/><path d="M232 100 L276 146" marker-end="url(#fb-teal)"/><path d="M276 100 L320 146" marker-end="url(#fb-teal)"/><path d="M320 100 L364 146" marker-end="url(#fb-teal)"/><path d="M364 100 L408 146" marker-end="url(#fb-teal)"/><path d="M408 100 L452 146" marker-end="url(#fb-teal)"/><path d="M452 100 L496 146" marker-end="url(#fb-teal)"/>
<path d="M188 182 L276 228" marker-end="url(#fb-teal)"/><path d="M232 182 L320 228" marker-end="url(#fb-teal)"/><path d="M276 182 L364 228" marker-end="url(#fb-teal)"/><path d="M320 182 L408 228" marker-end="url(#fb-teal)"/><path d="M364 182 L452 228" marker-end="url(#fb-teal)"/><path d="M408 182 L496 228" marker-end="url(#fb-teal)"/>
<path d="M188 264 L364 310" marker-end="url(#fb-teal)"/><path d="M232 264 L408 310" marker-end="url(#fb-teal)"/><path d="M276 264 L452 310" marker-end="url(#fb-teal)"/><path d="M320 264 L496 310" marker-end="url(#fb-teal)"/>
</g>
<g>
<rect x="170" y="68" width="36" height="32" rx="8" fill="#ffffff" stroke="#d6d3d1"/><text x="188" y="89" text-anchor="middle" class="t-label">3</text><rect x="214" y="68" width="36" height="32" rx="8" fill="#ffffff" stroke="#d6d3d1"/><text x="232" y="89" text-anchor="middle" class="t-label">1</text><rect x="258" y="68" width="36" height="32" rx="8" fill="#ffffff" stroke="#d6d3d1"/><text x="276" y="89" text-anchor="middle" class="t-label">4</text><rect x="302" y="68" width="36" height="32" rx="8" fill="#ffffff" stroke="#d6d3d1"/><text x="320" y="89" text-anchor="middle" class="t-label">1</text><rect x="346" y="68" width="36" height="32" rx="8" fill="#ffffff" stroke="#d6d3d1"/><text x="364" y="89" text-anchor="middle" class="t-label">5</text><rect x="390" y="68" width="36" height="32" rx="8" fill="#ffffff" stroke="#d6d3d1"/><text x="408" y="89" text-anchor="middle" class="t-label">9</text><rect x="434" y="68" width="36" height="32" rx="8" fill="#ffffff" stroke="#d6d3d1"/><text x="452" y="89" text-anchor="middle" class="t-label">2</text><rect x="478" y="68" width="36" height="32" rx="8" fill="#ffffff" stroke="#d6d3d1"/><text x="496" y="89" text-anchor="middle" class="t-label">6</text>
<rect x="170" y="150" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="188" y="171" text-anchor="middle" class="t-label">3</text><rect x="214" y="150" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="232" y="171" text-anchor="middle" class="t-label">4</text><rect x="258" y="150" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="276" y="171" text-anchor="middle" class="t-label">5</text><rect x="302" y="150" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="320" y="171" text-anchor="middle" class="t-label">5</text><rect x="346" y="150" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="364" y="171" text-anchor="middle" class="t-label">6</text><rect x="390" y="150" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="408" y="171" text-anchor="middle" class="t-label">14</text><rect x="434" y="150" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="452" y="171" text-anchor="middle" class="t-label">11</text><rect x="478" y="150" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="496" y="171" text-anchor="middle" class="t-label">8</text>
<rect x="170" y="232" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="188" y="253" text-anchor="middle" class="t-label">3</text><rect x="214" y="232" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="232" y="253" text-anchor="middle" class="t-label">4</text><rect x="258" y="232" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="276" y="253" text-anchor="middle" class="t-label">8</text><rect x="302" y="232" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="320" y="253" text-anchor="middle" class="t-label">9</text><rect x="346" y="232" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="364" y="253" text-anchor="middle" class="t-label">11</text><rect x="390" y="232" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="408" y="253" text-anchor="middle" class="t-label">19</text><rect x="434" y="232" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="452" y="253" text-anchor="middle" class="t-label">17</text><rect x="478" y="232" width="36" height="32" rx="8" fill="#f0fdfa" stroke="#0f766e"/><text x="496" y="253" text-anchor="middle" class="t-label">22</text>
<rect x="170" y="314" width="36" height="32" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="188" y="335" text-anchor="middle" class="t-label">3</text><rect x="214" y="314" width="36" height="32" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="232" y="335" text-anchor="middle" class="t-label">4</text><rect x="258" y="314" width="36" height="32" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="276" y="335" text-anchor="middle" class="t-label">8</text><rect x="302" y="314" width="36" height="32" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="320" y="335" text-anchor="middle" class="t-label">9</text><rect x="346" y="314" width="36" height="32" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="364" y="335" text-anchor="middle" class="t-label">14</text><rect x="390" y="314" width="36" height="32" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="408" y="335" text-anchor="middle" class="t-label">23</text><rect x="434" y="314" width="36" height="32" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="452" y="335" text-anchor="middle" class="t-label">25</text><rect x="478" y="314" width="36" height="32" rx="8" fill="#0f766e"/><text x="496" y="335" text-anchor="middle" class="t-white">31</text>
</g>
<g>
<rect x="548" y="122" width="140" height="164" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="566" y="148" class="t-title">图中如何读依赖</text>
<line x1="566" y1="170" x2="596" y2="170" stroke="#a8a29e" stroke-width="1.2"/><text x="606" y="174" class="t-sub">保留同一位置</text>
<line x1="566" y1="198" x2="596" y2="198" stroke="#0f766e" stroke-width="1.8" marker-end="url(#fb-teal)"/><text x="606" y="202" class="t-sub">读取左侧结果</text>
<text x="566" y="230" class="t-sub">跨度依次为</text><text x="566" y="250" class="t-label" fill="#0f766e" font-weight="600">1 → 2 → 4</text>
<text x="566" y="274" class="t-sub">同一行无横向箭头</text>
</g>
<text x="342" y="380" text-anchor="middle" class="t-sub">变量从 8 个增加为 4 组 × 8 个；同轮计算彼此独立，依赖深度由 7 缩短为 3</text>
</svg>
<figcaption class="fig-caption">灰线表示保留上一轮同一位置的结果，绿色斜线表示读取左侧结果。每一轮都产生一组新的中间变量；由于同一行没有依赖边，该行的所有位置可以并行计算。</figcaption>
</figure>

这种重排没有减少依赖和计算量。它增加了中间变量，依赖边也随之增加；变化在于，依赖不再集中在一条长链上，而是分散到三层彼此独立的并行计算中。作为代价，加法次数从 7 增加到 17，若完整保留每一轮结果，还需要更多存储空间。因此，更准确的说法是：**用更多中间状态和总工作量，换取更短的关键路径。**

真正压不掉的依赖长什么样？当这条链不是往前延伸，而是绕回来咬住自己的尾巴——那就是循环，也是静态 DAG 装不下的东西。第 3 节我们就去见它。

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答的问题是"为什么我加了机器，程序还是不快"。排查顺序是固定的：先找到关键路径，看自己卡在链上还是卡在宽度上。卡在链上，加机器没用——要么换算法把链压短，要么像 Hillis-Steele 这样拿工作量换深度。</p>
</div>

## 2. 从依赖关系到静态计算图

前面的两种算法都可以画成图。图中的节点表示一次计算，箭头表示计算之间的数据依赖。如果节点 B 需要读取节点 A 的结果，就从 A 向 B 画一条箭头。箭头同时规定了执行顺序：A 尚未完成，B 就不能开始。

只要图中不存在循环，这些依赖就构成一张有向无环图，即 DAG。所谓“无环”，是指沿着箭头一直前进，不会重新回到已经经过的节点。这样的计算一定能够结束：每完成一层，就会向最终结果推进一步。

求和树是一张 DAG，Hillis-Steele 扫描也是一张 DAG。二者的区别不在是否使用 DAG，而在 DAG 的形状。前者把多个输入逐层合并，后者增加中间变量，把一条较长的依赖链改写成多层并行分支。图的宽度表示同一时刻最多可以执行多少工作，图的深度则给出理想条件下至少需要多少轮。

这两张图还有一个共同特点：输入规模一旦确定，所有节点和依赖关系也随之确定。计算开始之前，我们已经能够画出整张图，并按照这张图分配任务。这类图可以称为静态计算图。

### 2.1 计算图与函数表达式

静态计算图并不是并行系统独有的表示方法。一个普通的函数表达式，也可以按照同样的方式展开。

例如，八个数的树形求和可以写成：

```text
sum8(a, b, c, d, e, f, g, h)
  = add(
      add(add(a, b), add(c, d)),
      add(add(e, f), add(g, h))
    )
```

每一次 `add` 都对应图中的一个节点；一个 `add` 的返回值被另一个 `add` 使用，就对应节点之间的一条依赖边。表达式最内层的四次加法互不依赖，可以并行执行；外层加法必须等待内层结果，因此位于下一层。

从这个角度看，函数表达式与计算图描述的是同一件事：函数表达式从语法上说明结果如何组合，计算图则把其中的数据依赖显式地画出来。对于没有副作用的纯函数，只要依赖关系得到满足，各节点采用何种先后顺序、由哪台机器执行，都不会改变最终结果。这正是并行调度能够成立的基础。

### 2.2 静态图的边界

静态计算图适合描述执行前已经能够确定的工作。SQL 执行计划、批处理作业以及由多个算子组成的数据处理流水线，通常都可以先生成一张静态 DAG，再交给执行引擎调度。

但是，DAG 中的字母 A 表示 acyclic，也就是无环。一旦某个计算需要把当前结果重新送回前面的步骤，图中就会出现回边。此时问题不再只是如何安排节点，而是还要回答两个问题：这条回边上的数据属于第几轮，以及计算应当在什么时候停止。

下一节从一个需要逐层展开的查询开始讨论这个问题。

## 3. 无法预先画完的计算图

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 760 420" role="img" aria-label="左：甲丙丁戊组成闭合持股环，戊以红色虚线反向持有甲；右：Δ输入、JOIN、去重、反馈组成闭合计算环，已知集合为侧向输出">
<defs>
<marker id="lp-orange" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#9a3412"/></marker>
<marker id="lp-red" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#b91c1c"/></marker>
<marker id="lp-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
<marker id="lp-gray" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#57534e"/></marker>
</defs>
<text x="24" y="30" class="t-title">业务数据：交叉持股形成环</text>
<text x="400" y="30" class="t-title">计算结构：反馈边形成环</text>
<line x1="380" y1="20" x2="380" y2="400" stroke="#e8e0d4" stroke-width="1" stroke-dasharray="2 4"/>
<g stroke="#9a3412" stroke-width="1.8" fill="none">
<line x1="203" y1="106" x2="290" y2="183" marker-end="url(#lp-orange)"/>
<line x1="290" y1="217" x2="203" y2="294" marker-end="url(#lp-orange)"/>
<line x1="167" y1="294" x2="80" y2="217" marker-end="url(#lp-orange)"/>
<line x1="65" y1="63" x2="161" y2="86" marker-end="url(#lp-orange)"/>
<path d="M58 50 C130 30, 260 30, 300 172" marker-end="url(#lp-orange)"/>
<line x1="185" y1="112" x2="185" y2="178" marker-end="url(#lp-orange)"/>
<line x1="207" y1="200" x2="284" y2="200" marker-end="url(#lp-orange)"/>
</g>
<line x1="80" y1="183" x2="167" y2="106" stroke="#b91c1c" stroke-width="2.2" stroke-dasharray="7 5" fill="none" marker-end="url(#lp-red)"/>
<g>
<circle cx="185" cy="90" r="22" fill="#ffffff" stroke="#9a3412" stroke-width="1.6"/><text x="185" y="95" text-anchor="middle" class="t-label">甲</text>
<circle cx="310" cy="200" r="22" fill="#ffffff" stroke="#9a3412" stroke-width="1.6"/><text x="310" y="205" text-anchor="middle" class="t-label">丙</text>
<circle cx="185" cy="310" r="22" fill="#ffffff" stroke="#9a3412" stroke-width="1.6"/><text x="185" y="315" text-anchor="middle" class="t-label">丁</text>
<circle cx="60" cy="200" r="22" fill="#ffffff" stroke="#b91c1c" stroke-width="1.6"/><text x="60" y="205" text-anchor="middle" class="t-label">戊</text>
<circle cx="185" cy="200" r="18" fill="#ffffff" stroke="#9a3412" stroke-width="1.4"/><text x="185" y="204" text-anchor="middle" class="t-label">乙</text>
<circle cx="45" cy="60" r="18" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4"/><text x="45" y="64" text-anchor="middle" class="t-label">P</text>
</g>
<g class="t-micro">
<text x="108" y="60">70%</text><text x="262" y="48">5%</text>
<text x="258" y="138">10%</text><text x="193" y="150">60%</text><text x="242" y="186">50%</text>
<text x="258" y="266">80%</text><text x="112" y="266">90%</text>
<text x="96" y="130" fill="#b91c1c">交叉持股</text>
</g>
<g stroke="#0f766e" stroke-width="2.2" fill="none">
<line x1="475" y1="177" x2="548" y2="120" marker-end="url(#lp-teal)"/>
<line x1="582" y1="120" x2="655" y2="177" marker-end="url(#lp-teal)"/>
<line x1="655" y1="223" x2="582" y2="280" marker-end="url(#lp-teal)"/>
<line x1="548" y1="280" x2="475" y2="223" marker-end="url(#lp-teal)"/>
</g>
<line x1="665" y1="223" x2="665" y2="306" stroke="#57534e" stroke-width="1.6" fill="none" marker-end="url(#lp-gray)"/>
<g>
<rect x="423" y="177" width="84" height="46" rx="12" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.5"/><text x="465" y="205" text-anchor="middle" class="t-label" font-weight="600">Δ 输入</text>
<rect x="523" y="72" width="84" height="46" rx="12" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.5"/><text x="565" y="100" text-anchor="middle" class="t-label" font-weight="600">JOIN</text>
<rect x="623" y="177" width="84" height="46" rx="12" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.5"/><text x="665" y="205" text-anchor="middle" class="t-label" font-weight="600">去重</text>
<rect x="523" y="282" width="84" height="46" rx="12" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.5"/><text x="565" y="310" text-anchor="middle" class="t-label" font-weight="600">反馈</text>
<rect x="623" y="310" width="84" height="40" rx="12" fill="#ffffff" stroke="#a8a29e" stroke-width="1.4"/><text x="665" y="334" text-anchor="middle" class="t-label">已知集合</text>
</g>
<g class="t-micro">
<text x="640" y="126">候选公司</text>
<text x="648" y="268">仅本轮新公司</text>
<text x="498" y="270" text-anchor="end">作为 Δ 返回</text>
<text x="665" y="370" text-anchor="middle" fill="#57534e">累计输出</text>
</g>
<text x="185" y="380" text-anchor="middle" class="t-sub">沿持股箭头查询：甲 → 丙 → 丁 → 戊 → 甲</text>
<text x="565" y="380" text-anchor="middle" class="t-sub">每轮执行同一个 join，去重后的新公司重新成为输入</text>
</svg>
<figcaption class="fig-caption">左：输入数据本身含有环——戊反过来持有甲（红色虚线），沿持股关系查询会回到起点。右：系统用一条反馈边重复执行同一个 join；去重只放行本轮新发现的公司，已知集合作为侧向输出累计结果。</figcaption>
</figure>

考虑上图中的持股关系：自然人 P 持有甲和丙；甲持有乙和丙；乙持有丙；丙持有丁，丁持有戊，而戊又持有甲。若要查询 P 直接和间接持有哪些公司，就必须把每轮新发现的公司再次作为股东，回到同一张表中检索。

这个过程会逐层进行：

| 轮次 | 本轮用于查询的股东 | 从持股表中找到的公司 | 新发现的公司 |
|---|---|---|---|
| 0 | P | 甲、丙 | 甲、丙 |
| 1 | 甲、丙 | 乙、丙、丁 | 乙、丁 |
| 2 | 乙、丁 | 丙、戊 | 戊 |
| 3 | 戊 | 甲 | 甲已经存在，无新增结果，计算结束 |

每一轮执行的操作都相同：把本轮新发现的公司与持股表连接，再从结果中去掉已经见过的公司。写成关系运算就是：

```text
新结果 = distinct(本轮新发现的公司 ⋈ holds) − 已知公司
```

其中，`holds` 是固定的持股关系表。变化的只是每一轮送入连接操作的公司集合。

### 3.1 静态图为什么不够

如果持股链的最大长度事先已知，例如确定最多只有三层，那么可以把三次连接操作直接写成一张静态 DAG。但是实际数据可能只有一层，也可能有十层；查询开始之前，程序通常无法知道需要展开多少轮。

一种办法是预先画出足够多的层数，例如固定执行十轮。这样虽然仍能得到部分场景中的正确结果，却同时带来两个问题：链较短时会产生多余计算，链超过十层时又会遗漏结果。静态展开只是把未知的循环次数替换成了人为设定的上限，并没有真正表达循环。

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 720 360" role="img" aria-label="固定四轮的静态展开图：数据两轮收敛时后两轮空转，数据六轮才收敛时答案落在图外">
<defs>
<marker id="s31-ink" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#57534e"/></marker>
<marker id="s31-red" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#b91c1c"/></marker>
</defs>
<text x="24" y="30" class="t-title">同一张静态图，遇到两种不同的数据</text>
<g class="t-sub">
<text x="24" y="80">静态展开（固定 4 轮）</text>
<text x="24" y="170">数据 A：第 2 轮收敛</text>
<text x="24" y="260">数据 B：第 6 轮才收敛</text>
</g>
<g stroke="#57534e" stroke-width="1.5" fill="none">
<line x1="266" y1="75" x2="280" y2="75" marker-end="url(#s31-ink)"/><line x1="378" y1="75" x2="392" y2="75" marker-end="url(#s31-ink)"/><line x1="490" y1="75" x2="504" y2="75" marker-end="url(#s31-ink)"/>
</g>
<g>
<rect x="170" y="56" width="96" height="38" rx="10" fill="#ffffff" stroke="#57534e" stroke-width="1.4"/><text x="218" y="80" text-anchor="middle" class="t-label">第 1 轮</text>
<rect x="282" y="56" width="96" height="38" rx="10" fill="#ffffff" stroke="#57534e" stroke-width="1.4"/><text x="330" y="80" text-anchor="middle" class="t-label">第 2 轮</text>
<rect x="394" y="56" width="96" height="38" rx="10" fill="#ffffff" stroke="#57534e" stroke-width="1.4"/><text x="442" y="80" text-anchor="middle" class="t-label">第 3 轮</text>
<rect x="506" y="56" width="96" height="38" rx="10" fill="#ffffff" stroke="#57534e" stroke-width="1.4"/><text x="554" y="80" text-anchor="middle" class="t-label">第 4 轮</text>
</g>
<g stroke="#0f766e" stroke-width="1.5" fill="none">
<line x1="266" y1="165" x2="280" y2="165" marker-end="url(#s31-ink)"/><line x1="378" y1="165" x2="392" y2="165" marker-end="url(#s31-ink)"/><line x1="490" y1="165" x2="504" y2="165" marker-end="url(#s31-ink)"/>
</g>
<g>
<rect x="170" y="146" width="96" height="38" rx="10" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="218" y="170" text-anchor="middle" class="t-label">第 1 轮</text>
<rect x="282" y="146" width="96" height="38" rx="10" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="330" y="170" text-anchor="middle" class="t-label">第 2 轮 ✓</text>
<rect x="394" y="146" width="96" height="38" rx="10" fill="#f6f1e7" stroke="#a8a29e" stroke-width="1.4" stroke-dasharray="5 4"/><text x="442" y="170" text-anchor="middle" class="t-sub">空转</text>
<rect x="506" y="146" width="96" height="38" rx="10" fill="#f6f1e7" stroke="#a8a29e" stroke-width="1.4" stroke-dasharray="5 4"/><text x="554" y="170" text-anchor="middle" class="t-sub">空转</text>
</g>
<text x="386" y="212" text-anchor="middle" class="t-label" fill="#9a3412" font-weight="600">太长：后两轮白算 —— 浪费，但结果正确</text>
<g stroke="#0f766e" stroke-width="1.5" fill="none">
<line x1="266" y1="255" x2="280" y2="255" marker-end="url(#s31-ink)"/><line x1="378" y1="255" x2="392" y2="255" marker-end="url(#s31-ink)"/><line x1="490" y1="255" x2="504" y2="255" marker-end="url(#s31-ink)"/>
</g>
<g>
<rect x="170" y="236" width="96" height="38" rx="10" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="218" y="260" text-anchor="middle" class="t-label">第 1 轮</text>
<rect x="282" y="236" width="96" height="38" rx="10" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="330" y="260" text-anchor="middle" class="t-label">第 2 轮</text>
<rect x="394" y="236" width="96" height="38" rx="10" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="442" y="260" text-anchor="middle" class="t-label">第 3 轮</text>
<rect x="506" y="236" width="96" height="38" rx="10" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="554" y="260" text-anchor="middle" class="t-label">第 4 轮</text>
</g>
<line x1="602" y1="255" x2="628" y2="255" stroke="#b91c1c" stroke-width="1.6" stroke-dasharray="5 4" fill="none" marker-end="url(#s31-red)"/>
<line x1="618" y1="228" x2="618" y2="292" stroke="#b91c1c" stroke-width="2" stroke-dasharray="6 5"/>
<text x="618" y="308" text-anchor="middle" class="t-micro" fill="#b91c1c">图的终点</text>
<rect x="632" y="240" width="76" height="30" rx="8" fill="#fee2e2" stroke="#b91c1c" stroke-width="1.4" stroke-dasharray="5 4"/><text x="670" y="259" text-anchor="middle" class="t-label" fill="#b91c1c">答案？</text>
<text x="386" y="330" text-anchor="middle" class="t-label" fill="#b91c1c" font-weight="600">太短：答案在第 6 轮，图在第 4 轮就结束了 —— 根本算不出来</text>
</svg>
<figcaption class="fig-caption">静态展开用人为的上限替代真实的迭代次数。链短时空转几轮，只是浪费；链长时答案落在图外，结果就是错的。两种失败不对称——真正不可接受的是太短。</figcaption>
</figure>

更直接的办法是加入一条回边，把本轮的新结果重新送回连接操作。此时，算子结构本身可以保持不变：同一个 join 被重复使用，不必为每一轮复制一套算子。但是图中出现回边以后，仅靠原来的依赖关系已经无法决定执行顺序。系统还必须知道一条记录属于哪一轮，以及未来是否还会有新的记录进入这一轮。

因此，循环需要补充两项机制：

1. **轮次或逻辑时间**：区分同一条回边上不同迭代产生的数据；
2. **进度判断**：确定某一轮是否已经结束，以及整个循环是否已经收敛。

这两个问题构成了后文比较同步迭代与 Timely Dataflow 的基础。

### 3.2 去重不仅影响性能，也决定能否终止

前面的例子中，丙公司会被发现三次：P 直接持有丙，甲持有丙，乙也持有丙。如果每次发现都不加区分地重新送入下一轮，即使没有环，也会产生重复计算。若持股关系中存在交叉持股，例如甲持有乙、乙又持有甲，问题会更加明显：记录会沿着环不断返回，计算永远不会停止。

因此，每一轮都要区分“已经见过的公司”和“本轮第一次发现的公司”。只有后者才需要进入下一轮。前面公式中的 `distinct` 和集合差并非单纯的性能优化，它们同时给出了集合型递归的终止条件：当一轮计算不再产生新公司时，已知集合达到不动点，循环结束。

这一点也说明了运行时展开的实质。系统并不是随意创建一张结构不断变化的图，而是在重复执行同一段计算结构；真正动态变化的是每轮输入的数据，以及由这些数据决定的迭代次数。

## 4. 表达循环的两条路线

上一节的结论是：循环需要两样东西——区分轮次的逻辑时间，和判断进度的机制。这两样东西可以放在两个地方：记在系统里，或者记在数据里。这个选择把系统分成了两类：一类用同步屏障把动态图切成一轮一轮来跑，一类不设屏障、让消息带着时间戳自由流动。这一章先看两种做法各自的实现，最后用同一条 SQL 把它们跑一遍。

### 4.1 同步轮次：把时间记在系统里

最直观的办法是让所有机器对齐"现在"。计算按轮次推进：第 k 轮里，所有节点并行处理第 k−1 轮的输出；全部完成后，一道全局屏障把系统锁齐，第 k+1 轮才能开始。轮次编号是系统的全局状态，数据本身不需要携带时间——所有人都在同一轮，时间是隐式的。

这条路线有很长的谱系。MPI 程序里，程序员手工插入屏障和集合通信（`MPI_Barrier`、`MPI_Allreduce`），同步点是代码的一部分。Pregel 把它自动化为 superstep：每个节点在每轮接收上轮消息、更新本地状态、发出新消息，框架负责轮末对齐。Flink 的 DataSet API 提供 bulk / delta iteration，图计算库 Gelly 在其上实现了同样的模型。

用上一节的持股查询推演。本轮待展开的公司记为 **Δ 集合**——有些材料叫它 frontier，但这个词在 4.2 有一个完全不同的精确含义，这里避免混用：

| superstep | Δ 集合 ⋈ holds | 轮末已知集合 | 本轮白做的功 |
|---|---|---|---|
| 0 | {P} → 甲, 丙 | {P, 甲, 丙} | — |
| 1 | {甲, 丙} → 乙, 丙（重复）, 丁 | +乙, +丁 | 甲→丙 重复发现，作废 |
| 2 | {乙, 丁} → 丙（重复）, 戊 | +戊 | 乙→丙 又作废一次 |
| 3 | {戊} → 甲（沿环回来，已知） | 不变 | 空转一轮，只为确认收敛 |

同步轮次给出一个干净的语义：第 k 轮结束时，已知集合恰好是所有不超过 k 跳可达的公司——每一轮末，系统处在一个全局一致的状态。这个性质有实际价值：**收敛判据可以是任意全局聚合**。例如 PageRank 的"本轮最大变化小于 ε"，在同步模型里只是一次全局归约。

代价同样写在表里：重复发现的消息（甲→丙、乙→丙、戊→甲）各自消耗了一轮的部分算力才被清算；确认收敛需要额外空转一轮；每一轮里，最慢的分区决定全系统的进度。

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 720 260" role="img" aria-label="同步轮次的泳道图：每轮末端有屏障，最慢的分区拖住其他分区等待">
<g class="t-sub">
<text x="8" y="76">worker 1</text><text x="8" y="116">worker 2</text><text x="8" y="156">worker 3</text><text x="8" y="196">worker 4</text>
</g>
<g text-anchor="middle">
<text x="190" y="32" class="t-title">superstep k</text>
<text x="450" y="32" class="t-title">superstep k+1</text>
</g>
<g>
<rect x="70" y="56" width="180" height="24" rx="6" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4"/>
<rect x="70" y="96" width="180" height="24" rx="6" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4"/>
<rect x="70" y="136" width="252" height="24" rx="6" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4"/>
<rect x="70" y="176" width="180" height="24" rx="6" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4"/>
<rect x="250" y="56" width="72" height="24" rx="6" fill="#f6f1e7" stroke="#e8e0d4"/><text x="286" y="72" text-anchor="middle" class="t-micro">等待</text>
<rect x="250" y="96" width="72" height="24" rx="6" fill="#f6f1e7" stroke="#e8e0d4"/><text x="286" y="112" text-anchor="middle" class="t-micro">等待</text>
<rect x="250" y="176" width="72" height="24" rx="6" fill="#f6f1e7" stroke="#e8e0d4"/><text x="286" y="192" text-anchor="middle" class="t-micro">等待</text>
<rect x="352" y="56" width="170" height="24" rx="6" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4" opacity="0.55"/>
<rect x="352" y="96" width="170" height="24" rx="6" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4" opacity="0.55"/>
<rect x="352" y="136" width="170" height="24" rx="6" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4" opacity="0.55"/>
<rect x="352" y="176" width="170" height="24" rx="6" fill="#ffedd5" stroke="#9a3412" stroke-width="1.4" opacity="0.55"/>
</g>
<g stroke="#b91c1c" stroke-width="2.5">
<line x1="334" y1="44" x2="334" y2="212"/><line x1="560" y1="44" x2="560" y2="212"/>
</g>
<g class="t-micro" fill="#b91c1c" text-anchor="middle">
<text x="334" y="228">屏障：等最慢的分区</text><text x="560" y="228">屏障</text>
</g>
<text x="196" y="152" text-anchor="middle" class="t-micro" fill="#9a3412" font-weight="600">straggler</text>
<text x="610" y="128" class="t-sub">下一轮整体开始</text>
</svg>
<figcaption class="fig-caption">同步轮次：每一轮末端有一道屏障，所有分区到齐后下一轮才能开始。worker 3 是本轮的 straggler，其余三个分区只能空等。</figcaption>
</figure>

关于实现现状需要说明一句：Flink 的同步迭代能力在 DataSet API 和 Gelly 中，而 DataSet API 正在被 DataStream 的批处理模式取代；`DataStream.iterate()` 并不提供 superstep 语义——没有屏障，也没有内建的终止检测——目前已被废弃。本节讨论的代表实现是 MPI、Pregel、Giraph、Spark GraphX 和 Gelly，而不是一个开箱即用的在线服务。

### 4.2 逻辑时间：把时间记在数据里

Timely Dataflow 做了相反的选择：不设全局屏障，每个算子收到消息就立即增量计算并继续输出。这样解决了“数据怎样持续向前流动”，却没有解决“外部什么时候可以拿到一个 epoch 的完整答案”。运行时不会打开 join、distinct 或循环算子的业务状态，替它们理解“现在是否已经算完”；它只认识算子按照进度协议公开出来的状态。即使输入端已经结束 epoch `e`，由 `e` 产生的工作仍可能缓存在算子中、飞行在网络上，或者继续沿循环回边传播。下面先看循环怎样把时间写进消息，再解释系统怎样从这些时间的进度状态确认：epoch `e` 的答案以后不会再变。

#### 4.2.1 嵌套时间戳：进入循环，就加一个坐标

先用一个坐标：给每条消息标上它属于第几轮，够吗？对单个循环够了。但真实计算里循环外面还有循环：输入一批接一批到来（批与批之间是 epoch），每一批内部可能要做迭代（iteration），迭代里面还可能再嵌套迭代。一个整数分不清"第 2 批的第 3 轮"和"第 3 批的第 2 轮"。

Timely 的办法是：**时间戳不是一个数，而是一个坐标序列**。进入一层循环作用域，就在末尾追加一个坐标；离开这层作用域，就把它弹掉。第 2 批数据的循环里，第 3 轮的消息时间戳是 `(2, 3)`；如果循环里再套一层循环，内层第 1 轮就是 `(2, 3, 1)`。

这个结构和函数调用栈完全同构：调用一层函数，压一帧；返回，弹一帧。时间戳的长度就是嵌套深度，每个坐标是那一层的局部计数器。

写法上注意，坐标是**扁平的序列**，不是嵌套的二元组：顶层 scope 里时间戳就是 `3`；进入 iterate 后变成 `(3, 0)`；再嵌套一层循环就是 `(3, 0, 0)`。每进入一层作用域，在末尾追加一个坐标；离开时弹出。由此得到一个重要事实：**iterate 之外的算子看不到 iteration 坐标**——无论下游还接着多少算子，它们收到的时间戳只有 `3`，轮次被完整封装在作用域内部。

回边在这个结构里扮演什么角色？**消息每绕回边一圈，最内层坐标加一**——`(2, 3)` 绕一圈变成 `(2, 4)`。这条规则有一个重要推论：消息沿环前进时，时间必然严格增大，不存在“时间在环上原地打转”的消息。也正因为时间沿回边严格前进，运行时之后才有可能判断：哪些旧轮次已经永远不会再出现。

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 760 600" role="img" aria-label="逻辑视图：时间戳分层只在 iterate 内部，输入压入坐标、离开弹出，下游不可见。物理视图：三条并行泳道，戊@(3,3) 在 (3,2) 最后一条重复消息到达之前开工，各 worker 的局部进度贡献持续汇总，不存在同步屏障">
<defs>
<marker id="ph-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
<marker id="ph-gray" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#57534e"/></marker>
<marker id="ph-purple" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#6d28d9"/></marker>
<marker id="ph-red" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#b91c1c"/></marker>
<marker id="ph-orange" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#9a3412"/></marker>
</defs>
<text x="30" y="26" class="t-title">逻辑视图：分阶段只发生在 iterate 内部</text>
<rect x="60" y="44" width="120" height="30" rx="9" fill="#ffffff" stroke="#57534e" stroke-width="1.4"/><text x="120" y="63" text-anchor="middle" class="t-label">输入 {P} @t=3</text>
<text x="190" y="63" class="t-sub">顶层 scope：时间戳就是 3</text>
<line x1="120" y1="74" x2="120" y2="102" stroke="#0f766e" stroke-width="2.2" marker-end="url(#ph-teal)"/>
<text x="140" y="94" class="t-micro" fill="#0f766e">压坐标 3 → (3, 0)</text>
<rect x="32" y="106" width="696" height="170" rx="14" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.2" stroke-dasharray="6 5"/>
<text x="48" y="128" class="t-sub" fill="#0f766e" font-weight="600">iterate scope · iteration 坐标只在这个框内存在</text>
<g>
<rect x="48" y="136" width="158" height="120" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.2" stroke-dasharray="4 3"/>
<text x="62" y="156" class="t-micro" fill="#0f766e" font-weight="700">(3,0)</text>
<text x="62" y="178" class="t-label">Δ₀ = {P}</text>
<text x="62" y="198" class="t-sub">⋈ holds</text>
<text x="62" y="220" class="t-label">→ 甲 @(3,1)</text>
<text x="62" y="241" class="t-label">→ 丙 @(3,1)</text>
</g>
<g>
<rect x="218" y="136" width="158" height="120" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.2" stroke-dasharray="4 3"/>
<text x="232" y="156" class="t-micro" fill="#0f766e" font-weight="700">(3,1)</text>
<text x="232" y="178" class="t-label">Δ₁ = {甲, 丙}</text>
<text x="232" y="198" class="t-sub">⋈ holds</text>
<text x="232" y="220" class="t-label">→ 乙, 丁 @(3,2)</text>
<text x="232" y="241" class="t-label" fill="#b91c1c">✗ 丙 已知</text>
</g>
<g>
<rect x="388" y="136" width="158" height="120" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.2" stroke-dasharray="4 3"/>
<text x="402" y="156" class="t-micro" fill="#0f766e" font-weight="700">(3,2)</text>
<text x="402" y="178" class="t-label">Δ₂ = {乙, 丁}</text>
<text x="402" y="198" class="t-sub">⋈ holds</text>
<text x="402" y="220" class="t-label">→ 戊 @(3,3)</text>
<text x="402" y="241" class="t-label" fill="#b91c1c">✗ 丙 已知</text>
</g>
<g>
<rect x="558" y="136" width="158" height="120" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.2" stroke-dasharray="4 3"/>
<text x="572" y="156" class="t-micro" fill="#0f766e" font-weight="700">(3,3)</text>
<text x="572" y="178" class="t-label">Δ₃ = {戊}</text>
<text x="572" y="198" class="t-sub">⋈ holds</text>
<text x="572" y="220" class="t-label" fill="#b91c1c">✗ 甲 已知</text>
<text x="572" y="241" class="t-label">∅ 停</text>
</g>
<g stroke="#0f766e" stroke-width="1.8" stroke-dasharray="5 4" fill="none">
<line x1="208" y1="196" x2="216" y2="196" marker-end="url(#ph-teal)"/>
<line x1="378" y1="196" x2="386" y2="196" marker-end="url(#ph-teal)"/>
<line x1="548" y1="196" x2="556" y2="196" marker-end="url(#ph-teal)"/>
</g>
<line x1="637" y1="256" x2="637" y2="286" stroke="#57534e" stroke-width="2.2" marker-end="url(#ph-gray)"/>
<text x="460" y="302" text-anchor="middle" class="t-micro">弹出坐标：(3, k) → 3</text>
<rect x="557" y="288" width="140" height="30" rx="9" fill="#ffffff" stroke="#57534e" stroke-width="1.4"/><text x="627" y="307" text-anchor="middle" class="t-label">输出 @t=3</text>
<text x="627" y="338" text-anchor="middle" class="t-sub">下游算子只见 epoch = 3，iteration 坐标不泄漏</text>
<line x1="20" y1="358" x2="740" y2="358" stroke="#e8e0d4"/>
<text x="30" y="384" class="t-title">物理视图：三个 worker 的并行时间轴（无阶段边界）</text>
<g stroke="#efe8db" stroke-width="1">
<line x1="50" y1="438" x2="730" y2="438"/>
<line x1="50" y1="490" x2="730" y2="490"/>
<line x1="50" y1="542" x2="730" y2="542"/>
</g>
<g class="t-sub" fill="#57534e">
<text x="10" y="426">worker 1</text>
<text x="10" y="478">worker 2</text>
<text x="10" y="530">worker 3</text>
</g>
<g fill="#6d28d9" opacity="0.07">
<rect x="60" y="404" width="248" height="38"/>
<rect x="60" y="456" width="408" height="38"/>
<rect x="60" y="508" width="370" height="38"/>
</g>
<g stroke="#0f766e" stroke-width="1.1" opacity="0.55" fill="none">
<line x1="120" y1="422" x2="145" y2="422" marker-end="url(#ph-teal)"/>
<path d="M 92 436 C 100 455, 140 465, 163 470" marker-end="url(#ph-teal)"/>
<line x1="201" y1="422" x2="245" y2="422" marker-end="url(#ph-teal)"/>
<line x1="221" y1="474" x2="265" y2="474" marker-end="url(#ph-teal)"/>
<line x1="321" y1="474" x2="372" y2="474" marker-end="url(#ph-teal)"/>
</g>
<g stroke="#b91c1c" stroke-width="1" opacity="0.45" stroke-dasharray="3 3" fill="none">
<path d="M 173 436 C 220 505, 380 512, 448 521" marker-end="url(#ph-red)"/>
<path d="M 273 436 C 330 500, 460 508, 514 520" marker-end="url(#ph-red)"/>
<path d="M 404 488 C 470 482, 540 486, 598 514" marker-end="url(#ph-red)"/>
</g>
<rect x="368" y="393" width="58" height="152" fill="#ccfbf1" opacity="0.3"/>
<text x="397" y="390" text-anchor="middle" class="t-micro" fill="#0f766e">重叠</text>
<g>
<rect x="64" y="408" width="56" height="28" rx="8" fill="#ffffff" stroke="#57534e" stroke-width="1.3"/><text x="92" y="426" text-anchor="middle" class="t-label">P</text><text x="92" y="434" text-anchor="middle" class="t-micro">(3,0)</text>
<rect x="145" y="408" width="56" height="28" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.3"/><text x="173" y="426" text-anchor="middle" class="t-label">甲</text><text x="173" y="434" text-anchor="middle" class="t-micro">(3,1)</text>
<rect x="245" y="408" width="56" height="28" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.3"/><text x="273" y="426" text-anchor="middle" class="t-label">乙</text><text x="273" y="434" text-anchor="middle" class="t-micro">(3,2)</text>
</g>
<g>
<rect x="165" y="460" width="56" height="28" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.3"/><text x="193" y="478" text-anchor="middle" class="t-label">丙</text><text x="193" y="486" text-anchor="middle" class="t-micro">(3,1)</text>
<rect x="265" y="460" width="56" height="28" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.3"/><text x="293" y="478" text-anchor="middle" class="t-label">丁</text><text x="293" y="486" text-anchor="middle" class="t-micro">(3,2)</text>
<rect x="372" y="460" width="50" height="28" rx="8" fill="#0f766e"/><text x="397" y="478" text-anchor="middle" class="t-white">戊</text><text x="397" y="486" text-anchor="middle" class="t-micro" fill="#ccfbf1">(3,3)</text>
</g>
<g>
<rect x="452" y="512" width="56" height="28" rx="8" fill="#fee2e2" stroke="#b91c1c" stroke-width="1.3" stroke-dasharray="4 3"/><text x="480" y="530" text-anchor="middle" class="t-label" fill="#b91c1c">丙 ✗</text><text x="480" y="538" text-anchor="middle" class="t-micro">(3,2)</text>
<rect x="518" y="512" width="56" height="28" rx="8" fill="#fee2e2" stroke="#b91c1c" stroke-width="1.3" stroke-dasharray="4 3"/><text x="546" y="530" text-anchor="middle" class="t-label" fill="#b91c1c">丙 ✗</text><text x="546" y="538" text-anchor="middle" class="t-micro">(3,3)</text>
<rect x="584" y="512" width="56" height="28" rx="8" fill="#fee2e2" stroke="#b91c1c" stroke-width="1.3" stroke-dasharray="4 3"/><text x="612" y="530" text-anchor="middle" class="t-label" fill="#b91c1c">甲 ✗</text><text x="612" y="538" text-anchor="middle" class="t-micro">(3,4)</text>
</g>
<line x1="440" y1="393" x2="440" y2="548" stroke="#9a3412" stroke-width="2.2" stroke-dasharray="8 6"/>
<text x="448" y="392" class="t-micro" fill="#9a3412" font-weight="700">BSP 屏障（本图不存在）</text>
<g stroke="#6d28d9" stroke-width="1.5" stroke-dasharray="4 4" opacity="0.35">
<line x1="210" y1="408" x2="210" y2="438"/>
<line x1="330" y1="460" x2="330" y2="490"/>
<line x1="380" y1="512" x2="380" y2="542"/>
</g>
<g stroke="#6d28d9" stroke-width="1.4" fill="none" opacity="0.8">
<line x1="214" y1="444" x2="304" y2="444" marker-end="url(#ph-purple)"/>
<line x1="334" y1="496" x2="464" y2="496" marker-end="url(#ph-purple)"/>
<line x1="384" y1="548" x2="426" y2="548" marker-end="url(#ph-purple)"/>
</g>
<g stroke="#6d28d9" stroke-width="2" stroke-dasharray="4 4">
<line x1="308" y1="408" x2="308" y2="438"/>
<line x1="468" y1="460" x2="468" y2="490"/>
<line x1="430" y1="512" x2="430" y2="542"/>
</g>
<text x="380" y="564" text-anchor="middle" class="t-sub">箭头：消息到达即触发计算；紫色表示各 worker 的局部进度贡献——互不对齐，持续汇总</text>
<g class="t-sub" fill="#57534e">
<rect x="60" y="580" width="10" height="10" fill="#0f766e"/><text x="76" y="589">新结果立即流动</text>
<rect x="192" y="580" width="10" height="10" fill="#fee2e2" stroke="#b91c1c" stroke-dasharray="2 2"/><text x="208" y="589">重复就地吸收</text>
<line x1="328" y1="580" x2="328" y2="590" stroke="#6d28d9" stroke-width="2" stroke-dasharray="3 2"/><text x="336" y="589">局部进度贡献</text>
<line x1="456" y1="580" x2="476" y2="580" stroke="#9a3412" stroke-width="2" stroke-dasharray="5 3"/><text x="484" y="589">BSP 屏障（不存在）</text>
</g>
</svg>
<figcaption class="fig-caption">逻辑视图：时间戳的分层只存在于 iterate scope 内部——输入为 t=3，进入时压入坐标变成 (3,0)，内部按 (3,0)…(3,3) 分阶段（每层标出真实数据），离开时弹出坐标，下游算子只见 t=3。物理视图：同一批消息分布在三个 worker 上按物理时间并行处理——每条消息到达即触发下一条计算（细箭头；红色虚线是产生重复消息的触发），戊@(3,3) 在 (3,2) 的最后一条重复消息到达之前就已开工；紫色标记表示各 worker 提交的局部进度计数，它们持续汇总，却不要求 worker 在物理时间上对齐。橙色虚线是 BSP 会设置的屏障位置，这张图里不存在。下一节再解释这些计数如何变成可靠的完成判定。</figcaption>
</figure>

#### 4.2.2 输入结束不等于答案完成：系统如何感知 epoch 已完成

假设 source 已经宣布：epoch <code>e</code> 的输入全部发送完毕。这句话只关闭了**外部输入**，并没有关闭由它派生出来的内部工作。某条 <code>e</code> 的消息可能刚进入 join；join 可能把结果发给下一算子；下一算子又可能把结果送回循环。每个算子都在正常地增量计算，但没有任何一个瞬间能让运行时仅凭“当前队列为空”断言整个 epoch 已经结束。

而用户真正需要的是另一条承诺：**这个 epoch 的所有影响已经传播完，当前看到的答案以后不会再改变。** Timely 不靠理解算子内部的业务状态得到这条承诺，而是让所有消息和未来发送权都携带逻辑时间，再从时间的进度状态推导答案是否完整。准确地说，**答案内容由算子增量算出；时间进度不负责计算答案，只负责证明当前答案已经完整。**

先用单个整数时间建立直觉。若某个输入端口的 frontier 已推进到 <code>{f}</code>，就表示这个端口以后只可能收到时间不早于 <code>f</code> 的消息，因此所有 <code>t &lt; f</code> 的输入都已关闭。算子在这些时间上的结果可能早已作为增量向下游流动；frontier 到达 <code>f</code> 并不是此刻才产生结果，而是把“当前结果”升级成一条完成保证：时间 <code>t</code> 的答案已经完整，相关状态可以回收。

只有一个整数时，“早于 <code>f</code>”很好判断。进入二重循环后，时间变成多个坐标，可能出现互不可比的进展方向。为了把同一条完成保证推广到这种情况，运行时才需要比较时间。它要回答的不是“哪条日志先发生”，而是一个与答案完整性直接相关的问题：

> **某项尚未结束的工作，沿数据流继续执行后，还可能影响目标端口上的时间 <code>t</code> 吗？**

在普通直线数据流里，一个整数时间大多够用；在二重循环里，答案取决于工作绕过哪一层回边。设时间写成 <code>(e, o, i)</code>：<code>e</code> 是输入 epoch，<code>o</code> 是外层轮次，<code>i</code> 是内层轮次。进入一层循环就追加一个零，绕该层反馈边就把对应坐标加一，离开该层则弹掉最后一个坐标。

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 800 575" role="img" aria-label="二重循环中的时间变换：输入 epoch 进入外层追加 outer 坐标，进入内层追加 inner 坐标；内层和外层各有自己的 feedback 与 leave 算子">
<defs>
<marker id="nested-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
<marker id="nested-gray" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#57534e"/></marker>
</defs>
<text x="28" y="28" class="t-title">一个二重循环：坐标由走过的 scope 边界决定</text>
<rect x="28" y="46" width="744" height="500" rx="16" fill="#fafaf9" stroke="#57534e" stroke-width="1.2" stroke-dasharray="7 5"/>
<text x="45" y="70" class="t-sub" fill="#57534e" font-weight="700">外层 iterate scope</text>

<rect x="48" y="88" width="96" height="48" rx="9" fill="#ffffff" stroke="#57534e" stroke-width="1.2"/>
<text x="96" y="109" text-anchor="middle" class="t-label">输入</text>
<text x="96" y="127" text-anchor="middle" class="t-micro">@ e</text>

<rect x="170" y="82" width="120" height="60" rx="10" fill="#ffffff" stroke="#57534e" stroke-width="1.3"/>
<text x="230" y="105" text-anchor="middle" class="t-label">enter_outer</text>
<text x="230" y="126" text-anchor="middle" class="t-micro">e → (e, 0)</text>

<rect x="320" y="82" width="112" height="60" rx="10" fill="#ede9fe" stroke="#6d28d9" stroke-width="1.5"/>
<text x="376" y="105" text-anchor="middle" class="t-label" fill="#6d28d9">外层入口 O</text>
<text x="376" y="126" text-anchor="middle" class="t-micro">(e, o)</text>

<line x1="144" y1="112" x2="166" y2="112" stroke="#57534e" stroke-width="1.8" marker-end="url(#nested-gray)"/>
<line x1="290" y1="112" x2="316" y2="112" stroke="#57534e" stroke-width="1.8" marker-end="url(#nested-gray)"/>

<rect x="168" y="170" width="568" height="224" rx="14" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.3" stroke-dasharray="6 5"/>
<text x="186" y="194" class="t-sub" fill="#0f766e" font-weight="700">内层 iterate scope</text>

<rect x="192" y="218" width="124" height="62" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.3"/>
<text x="254" y="242" text-anchor="middle" class="t-label">enter_inner</text>
<text x="254" y="263" text-anchor="middle" class="t-micro">(e, o) → (e, o, 0)</text>

<rect x="344" y="218" width="104" height="62" rx="10" fill="#ede9fe" stroke="#6d28d9" stroke-width="1.6"/>
<text x="396" y="242" text-anchor="middle" class="t-label" fill="#6d28d9">内层入口 P</text>
<text x="396" y="263" text-anchor="middle" class="t-micro">(e, o, i)</text>

<rect x="492" y="218" width="164" height="62" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.4"/>
<text x="574" y="242" text-anchor="middle" class="t-label">inner body / result</text>
<text x="574" y="263" text-anchor="middle" class="t-micro">产生差分或不再产生</text>

<line x1="316" y1="249" x2="340" y2="249" stroke="#0f766e" stroke-width="2" marker-end="url(#nested-teal)"/>
<line x1="448" y1="249" x2="488" y2="249" stroke="#0f766e" stroke-width="2" marker-end="url(#nested-teal)"/>
<path d="M 544 280 L 544 306 L 396 306 L 396 284" fill="none" stroke="#0f766e" stroke-width="2.2" marker-end="url(#nested-teal)"/>
<text x="470" y="326" text-anchor="middle" class="t-micro" fill="#0f766e">inner feedback：(e, o, i) → (e, o, i + 1)</text>

<rect x="492" y="338" width="164" height="42" rx="9" fill="#ffffff" stroke="#57534e" stroke-width="1.3"/>
<text x="574" y="356" text-anchor="middle" class="t-label">leave_inner</text>
<text x="574" y="373" text-anchor="middle" class="t-micro">(e, o, i) → (e, o)</text>
<line x1="618" y1="280" x2="618" y2="334" stroke="#57534e" stroke-width="1.8" marker-end="url(#nested-gray)"/>

<path d="M 376 146 L 376 176 L 254 176 L 254 214" fill="none" stroke="#0f766e" stroke-width="2" marker-end="url(#nested-teal)"/>

<rect x="482" y="430" width="148" height="60" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.4"/>
<text x="556" y="453" text-anchor="middle" class="t-label">outer body / result</text>
<text x="556" y="474" text-anchor="middle" class="t-micro">@ (e, o)</text>
<line x1="574" y1="380" x2="574" y2="426" stroke="#0f766e" stroke-width="2" marker-end="url(#nested-teal)"/>

<rect x="660" y="430" width="92" height="60" rx="10" fill="#ffffff" stroke="#57534e" stroke-width="1.3"/>
<text x="706" y="453" text-anchor="middle" class="t-label">leave_outer</text>
<text x="706" y="474" text-anchor="middle" class="t-micro">(e, o) → e</text>
<line x1="630" y1="460" x2="656" y2="460" stroke="#57534e" stroke-width="1.8" marker-end="url(#nested-gray)"/>
<text x="706" y="514" text-anchor="middle" class="t-micro">输出 @e</text>

<path d="M 530 490 L 530 520 L 456 520 L 456 154 L 376 154 L 376 146" fill="none" stroke="#0f766e" stroke-width="2.2" marker-end="url(#nested-teal)"/>
<text x="292" y="523" class="t-micro" fill="#0f766e">outer feedback：(e, o) → (e, o + 1)</text>
</svg>
<figcaption class="fig-caption">数据离开循环必须经过独立的 <code>leave_inner</code> 或 <code>leave_outer</code>，不会从入口“倒着出去”。内层回边只增加 <code>i</code>，外层回边先弹出 <code>i</code>、增加 <code>o</code>，再次进入内层时再追加新的 <code>i=0</code>。</figcaption>
</figure>

现在沿图跟一项工作走一遍。内层结果位于 <code>(1, 2, 4)</code>，若它结束本次内层循环、参加下一轮外层循环，再次进入内层入口 P，时间会依次变成：

<pre><code>(1, 2, 4) --leave_inner--> (1, 2)
          --outer feedback--> (1, 3)
          --enter_inner-----> (1, 3, 0) @ P</code></pre>

因此，不同位置上的原始时间不能直接比较。运行时先把未完成工作的时间沿路径换算到**同一个目标端口**，再判断它是否可能影响目标时间。

这里以**当前 Rust Timely 实现**为准。嵌套的 <code>iterative</code> scope 使用嵌套的 <code>Product</code> 时间；<code>Product::less_equal</code> 要求 outer 和 inner 都不大于目标。因此，把嵌套结构摊平成三坐标后，裸时间采用逐坐标偏序：

<pre><code>(e₁, o₁, i₁) ≤ (e₂, o₂, i₂)
当且仅当 e₁≤e₂、o₁≤o₂、i₁≤i₂</code></pre>

> **版本说明：** 2013 年 Naiad 原论文把一个 epoch 内的循环计数向量写成字典序；当前 Rust Timely 则专门定义了 <code>Product</code>，使用乘积偏序。本文讨论当前代码，所以采用上面的逐坐标比较，不能把论文中的字典序直接套进来。

这里一定要分清两个步骤，它们不是同一种“比大小”：

1. **先走路径。** enter、leave 和 feedback 根据数据实际会经过的图结构改写时间戳；
2. **再比较。** 工作被换算到目标端口后，才用 <code>Product::less_equal</code> 的逐坐标偏序比较它与目标时间。

原论文把“时间 + 数据流位置”称为 pointstamp。设一项工作位于 <code>(t₁, l₁)</code>，目标位于 <code>(t₂, l₂)</code>；只有找到一条从 <code>l₁</code> 到 <code>l₂</code> 的路径 <code>ψ</code>，先按路径把时间变成 <code>ψ(t₁)</code>，再满足 <code>ψ(t₁) ≤ t₂</code>，才能说前者可能影响后者。

<pre><code>(t₁, l₁) could-result-in (t₂, l₂)
⇔ 存在路径 ψ：l₁ → l₂，并且 ψ(t₁) ≤ t₂</code></pre>

因此，按裸坐标看，<code>(1, 3, 1)</code> 与 <code>(1, 2, 4)</code> 确实不可比较；但这不表示两项工作互相不能影响。在这张二重循环图中，<code>(1, 2, 4)</code> 先沿 <code>leave_inner → outer feedback → enter_inner</code> **变成** P@<code>(1, 3, 0)</code>，然后运行时比较的是 <code>(1, 3, 0)</code> 与目标 <code>(1, 3, 1)</code>。因为前者逐坐标小于等于后者，所以原来的工作仍然可能影响 P@<code>(1, 3, 1)</code>。

如果这里使用的是字典序，根本不需要先走这条路径：它会直接判定 <code>(1, 2, 4) &lt; (1, 3, 1)</code>。当前 Timely 没有这样做；路径变换和乘积偏序是两个独立环节。

运行时必须先考虑工作所在的位置和可走的路径，再在目标端口比较换算后的时间。只要 <code>(1, 2, 4)</code> 仍可能走上述路径，P 的 frontier 就必须保留 <code>(1, 3, 0)</code> 这一方向；它不能越过这个点推进到 <code>(1, 3, 1)</code>。

到这里我们只解决了“怎样判断一项工作会不会影响目标时间”。还缺最后一块：运行时怎样知道这些尚未结束的工作确实存在，以及怎样把大量工作压缩成算子能使用的完成边界。

#### 4.2.3 一个负责“还能发”，一个负责“已经收完”：Capability 与 frontier

先只看两个算子：A 的输出连到 B 的输入。

<pre><code>算子 A  ────── msg@5 ──────►  算子 B
  输出端                         输入端</code></pre>

A 收到时间 5 的数据后，不一定马上输出。它可能要等异步请求、等另一个输入，或者把结果留到下一次调度再发。此时网络中可以一条消息都没有，但 A 仍然可能在将来发送时间 5 的数据。为了把这件事告诉运行时，A 必须保留一张“我还可以发送时间 5”的凭证，这就是 <code>cap@5</code>。

所以 **capability 在发送方手里使用**：

- A 以后还要发送时间 5 的数据，就继续持有 <code>cap@5</code>；
- A 已经处理完时间 5，以后只会发送时间 6 及其后的数据，就把 capability 的时间推进到 <code>cap@6</code>；
- A 再也不需要发送数据，就释放 capability。

Timely 把第二个操作命名为 <code>downgrade</code>。这里“推进”的是时间，“降级”的是发送权限：<code>cap@5</code> 还能保留发送时间 5 的可能，<code>cap@6</code> 已经放弃了这种权利，所以数字虽然变大，能力反而更弱。

B 关心的是另一件事：**时间 5 的输入是不是已经全部到齐？** B 不应该逐个检查所有上游算子持有什么 capability，也不应该只看自己的队列是否暂时为空。Timely 把所有上游 capability 和仍在路上的消息汇总成 B 输入端口的 frontier，B 只需要观察这条收件进度。

所以 **frontier 在接收方用来判断输入进度**。例如 B 的 frontier 从 <code>{5}</code> 推进到 <code>{6}</code>，表示时间 5 的数据已经全部收完，不会再来。B 这时才能确认时间 5 的结果完整，或者清理时间 5 的状态。

两者不是二选一，也不是同一份状态的两个名字：

| 角色 | 它回答的问题 | 谁来操作 |
| --- | --- | --- |
| capability | “我以后还能发送哪个时间的数据？” | 输出数据的算子持有、把时间推进（<code>downgrade</code>）或释放 |
| frontier | “这个输入端口已经收完哪些时间的数据？” | 运行时计算，接收数据的算子读取 |

许多简单的 <code>map</code>、<code>filter</code> 收到数据就立即输出，不需要把 capability 留到以后，也不需要主动查看 frontier。需要延迟输出的算子才要保存 capability；需要等一个时间的输入全部到齐，例如窗口、聚合、状态回收或循环不动点判断，才要观察 frontier。

同一个有状态算子经常两者都用。以时间 5 的窗口聚合为例：

1. 窗口还在收数据时，算子保存 <code>cap@5</code>，给自己保留稍后发送窗口结果的权利；
2. 算子观察输入 frontier；当它越过 5，说明窗口 5 的输入已经全部收完；
3. 算子用 <code>cap@5</code> 发送最终聚合结果，然后释放它，或者把它的时间推进到更晚。

这里，frontier 决定“什么时候可以结束等待”，capability 决定“结束等待后还能不能把结果发在时间 5”。

**用一条消息看清两者怎样接上。**

1. A 持有 <code>cap@5</code>。即使还没有消息，A 将来仍可能发送时间 5，因此 B 不能说时间 5 已经收完。
2. A 发送一条 <code>msg@5</code>，随后释放 <code>cap@5</code>。B 的 frontier 仍不能推进，因为这条消息还在网络里或 B 的输入队列中。
3. B 消费了最后一条 <code>msg@5</code>，同时也没有其他算子保留能产生时间 5 的 capability。到这时，时间 5 才真正收完，B 的 frontier 才能越过 5。

这条因果关系可以记成一句话：

> **上游用 capability 说明“我还可能发”；下游用 frontier 判断“我已经收完”。**

#### 4.2.4 Timely 怎样从 capability 和在途消息算出 frontier

frontier 本身不会作为一条控制消息从 A 复制给 B。运行时记录的是各个位置、各个时间还有多少份“未完成证据”：持有 capability 算一份，尚未消费的消息也算一份。创建或复制 capability、发送消息会增加相应计数，释放 capability、消费消息会减少相应计数；降级 capability 则是旧时间减一、新时间加一。

这里没有一个中央控制系统订阅所有算子的数据。算子和数据通道旁的运行时代码会自动记下 capability、消息产生和消息消费带来的计数增减；算子业务代码只需正确地保留、推进或释放 capability，不需要另外发送“我完成了”的控制消息。

每个 worker 先把本地计数变化攒成一批，再由 <code>Progcaster</code> 通过独立的进度通道广播给同一 scope 的其他 worker。广播的不是业务数据，也不是整个 frontier，而是 <code>(位置, 时间, 计数增减)</code>。每个 worker 收到各方变化后，都在本地运行 reachability tracker：先找出各位置仍有正计数的最早 pointstamp，再沿数据流路径推导它们对目标端口意味着哪些时间下界。普通边不改变时间；feedback、<code>enter</code> 和 <code>leave</code> 会按照路径改变循环时间。目标端口用 <code>MutableAntichain</code> 只保留这些下界中逐坐标最小、互相不可比较的点，这才是该端口的 frontier。

算子可以声明自己是否关心某个输入的 frontier 变化：<code>Never</code>、<code>IfCapability</code> 或 <code>Always</code>。这更接近“订阅”，但它只决定 frontier 改变时要不要唤醒该算子；即使算子选择 <code>Never</code>，运行时仍然会维护这个端口的 frontier。

<figure class="fig-card" id="frontier-propagation">
<svg class="fig-svg" viewBox="0 0 760 470" role="img" aria-label="frontier 不直接沿数据边传递。capability 和消息的产生、消费形成带位置与时间的计数增减，跨 worker 汇总后沿路径摘要传播，目标端口从正计数时间的最小反链重新计算 frontier。">
<defs>
<marker id="prop-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
<marker id="prop-purple" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#6d28d9"/></marker>
<marker id="prop-gray" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#57534e"/></marker>
</defs>
<text x="28" y="28" class="t-title">frontier 不是复制给下游，而是在每个位置重新算出来</text>

<text x="42" y="62" class="t-sub" font-weight="700">数据面：消息正常流动</text>
<rect x="42" y="78" width="142" height="58" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.3"/>
<text x="113" y="101" text-anchor="middle" class="t-label">算子 A · output</text>
<text x="113" y="122" text-anchor="middle" class="t-micro">持有 cap@5</text>
<rect x="298" y="78" width="142" height="58" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.3"/>
<text x="369" y="101" text-anchor="middle" class="t-label">网络中的 msg@5</text>
<text x="369" y="122" text-anchor="middle" class="t-micro">尚未被消费</text>
<rect x="554" y="78" width="164" height="58" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.3"/>
<text x="636" y="101" text-anchor="middle" class="t-label">算子 B · input</text>
<text x="636" y="122" text-anchor="middle" class="t-micro">收到后增量计算</text>
<line x1="184" y1="107" x2="294" y2="107" stroke="#0f766e" stroke-width="2" marker-end="url(#prop-teal)"/>
<line x1="440" y1="107" x2="550" y2="107" stroke="#0f766e" stroke-width="2" marker-end="url(#prop-teal)"/>

<line x1="24" y1="158" x2="736" y2="158" stroke="#e7e5e4"/>
<text x="42" y="184" class="t-sub" font-weight="700">进度跟踪：算子和通道旁的运行时代码自动记账</text>

<rect x="42" y="202" width="170" height="100" rx="11" fill="#fafaf9" stroke="#57534e" stroke-width="1.2"/>
<text x="58" y="226" class="t-label">本 worker 累计变化</text>
<text x="58" y="250" class="t-micro">A.out：cap@5　+1 / -1</text>
<text x="58" y="272" class="t-micro">B.in：msg@5　+1 / -1</text>
<text x="58" y="292" class="t-micro">只报告增加或减少，不传整个 frontier</text>

<rect x="250" y="202" width="164" height="100" rx="11" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.2"/>
<text x="332" y="226" text-anchor="middle" class="t-label">worker 之间广播</text>
<text x="332" y="252" text-anchor="middle" class="t-micro">Progcaster 交换计数变化</text>
<text x="332" y="274" text-anchor="middle" class="t-micro">各 worker 收到后各自计算</text>
<text x="332" y="294" text-anchor="middle" class="t-micro">没有中央控制器，也不是屏障</text>

<rect x="452" y="202" width="126" height="100" rx="11" fill="#ffffff" stroke="#57534e" stroke-width="1.2"/>
<text x="515" y="226" text-anchor="middle" class="t-label">沿数据流换算</text>
<text x="515" y="252" text-anchor="middle" class="t-micro">普通边：t → t</text>
<text x="515" y="274" text-anchor="middle" class="t-micro">feedback：t → t+1</text>
<text x="515" y="294" text-anchor="middle" class="t-micro">enter / leave：压入 / 弹出</text>

<rect x="616" y="202" width="102" height="100" rx="11" fill="#ede9fe" stroke="#6d28d9" stroke-width="1.3"/>
<text x="667" y="226" text-anchor="middle" class="t-label" fill="#6d28d9">目标端口</text>
<text x="667" y="252" text-anchor="middle" class="t-micro">找出还没结束的</text>
<text x="667" y="274" text-anchor="middle" class="t-micro">最早时间</text>
<text x="667" y="294" text-anchor="middle" class="t-label" fill="#6d28d9">得到 frontier</text>

<line x1="212" y1="252" x2="246" y2="252" stroke="#57534e" stroke-width="1.8" marker-end="url(#prop-gray)"/>
<line x1="414" y1="252" x2="448" y2="252" stroke="#57534e" stroke-width="1.8" marker-end="url(#prop-gray)"/>
<line x1="578" y1="252" x2="612" y2="252" stroke="#6d28d9" stroke-width="1.8" marker-end="url(#prop-purple)"/>

<text x="42" y="338" class="t-sub" font-weight="700">为什么释放 capability 后 frontier 还可能不动</text>
<rect x="42" y="354" width="208" height="74" rx="10" fill="#ffffff" stroke="#e7e5e4" stroke-width="1.2"/>
<text x="146" y="380" text-anchor="middle" class="t-label">① cap@5 +1</text>
<text x="146" y="406" text-anchor="middle" class="t-micro">A 仍可能发送，frontier 被 5 支撑</text>
<rect x="276" y="354" width="208" height="74" rx="10" fill="#ffffff" stroke="#e7e5e4" stroke-width="1.2"/>
<text x="380" y="380" text-anchor="middle" class="t-label">② 发送 msg@5；随后释放 cap</text>
<text x="380" y="406" text-anchor="middle" class="t-micro">消息仍在途，5 的正计数仍未归零</text>
<rect x="510" y="354" width="208" height="74" rx="10" fill="#ede9fe" stroke="#6d28d9" stroke-width="1.2"/>
<text x="614" y="380" text-anchor="middle" class="t-label" fill="#6d28d9">③ B 消费最后一条 msg@5</text>
<text x="614" y="406" text-anchor="middle" class="t-micro">5 的计数归零，frontier 才能推进</text>
<line x1="250" y1="391" x2="272" y2="391" stroke="#6d28d9" stroke-width="1.8" marker-end="url(#prop-purple)"/>
<line x1="484" y1="391" x2="506" y2="391" stroke="#6d28d9" stroke-width="1.8" marker-end="url(#prop-purple)"/>
</svg>
<figcaption class="fig-caption">数据边负责传递业务记录；进度通道在 worker 之间广播“哪个位置、哪个时间增加或减少了几份未完成证据”。没有中央控制器订阅所有数据，每个 worker 都根据收到的计数变化更新自己的 reachability tracker，再为各输入端口算出 frontier。对应到 Timely 代码，这条链路是 <code>SharedProgress → Progcaster → reachability tracker → MutableAntichain</code>。</figcaption>
</figure>

上面的 A → B 是一条直线。放进二重循环后，两者的用法没有变化：**可能向循环入口 P 继续发送数据的分支持有 capability，P 则通过自己的 frontier 判断某一轮数据是否已经收完。** 唯一多出来的步骤，是运行时必须先把各条路径上的 capability 和消息换算成它们到达 P 时的时间。

继续观察内层入口 P。假设有两条分支以后仍可能向它发送数据：

- A：<code>leave_inner</code> 之后的外层继续分支持有 <code>cap@(1, 2)</code>。沿 <code>outer feedback → enter_inner</code> 投影到 P，候选时间是 <code>(1, 3, 0)</code>；
- B：专门通向内层回边的分支持有 <code>cap@(1, 3, 0)</code>。沿 inner feedback 投影到 P，候选时间是 <code>(1, 3, 1)</code>。

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 800 330" role="img" aria-label="capability 和在途消息先沿路径投影成内层入口 P 的候选时间，最小候选构成 frontier；支撑最早点的工作消失后 frontier 推进">
<defs>
<marker id="cap-purple" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#6d28d9"/></marker>
<marker id="cap-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
</defs>
<text x="28" y="28" class="t-title">从未来工作的证据，到 P 的完成边界</text>

<rect x="28" y="52" width="220" height="162" rx="13" fill="#fafaf9" stroke="#57534e" stroke-width="1.2"/>
<text x="46" y="78" class="t-title">1　未来工作的证据</text>
<text x="46" y="108" class="t-label" fill="#6d28d9">A　外层分支 cap@(1, 2)</text>
<text x="46" y="130" class="t-micro">走 outer feedback 后还能到 P</text>
<text x="46" y="162" class="t-label" fill="#6d28d9">B　cap@(1, 3, 0)</text>
<text x="46" y="184" class="t-micro">走内层 feedback 后还能到 P</text>
<text x="46" y="204" class="t-micro">在途消息也按同样方式计数</text>

<rect x="290" y="52" width="220" height="162" rx="13" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.3"/>
<text x="308" y="78" class="t-title" fill="#0f766e">2　沿路径投影到 P</text>
<text x="308" y="112" class="t-label">A → (1, 3, 0)　×1</text>
<text x="308" y="146" class="t-label">B → (1, 3, 1)　×1</text>
<text x="308" y="184" class="t-micro">候选时间带计数；同一点可有多份支撑</text>

<rect x="552" y="52" width="220" height="162" rx="13" fill="#ede9fe" stroke="#6d28d9" stroke-width="1.4"/>
<text x="570" y="78" class="t-title" fill="#6d28d9">3　只保留最小候选</text>
<text x="662" y="126" text-anchor="middle" class="t-title" fill="#6d28d9">frontier(P)</text>
<text x="662" y="158" text-anchor="middle" class="t-title">{(1, 3, 0)}</text>
<text x="662" y="190" text-anchor="middle" class="t-micro">(1, 3, 1) 被更早的点覆盖</text>

<line x1="248" y1="133" x2="286" y2="133" stroke="#0f766e" stroke-width="2" marker-end="url(#cap-teal)"/>
<line x1="510" y1="133" x2="548" y2="133" stroke="#6d28d9" stroke-width="2" marker-end="url(#cap-purple)"/>

<rect x="28" y="250" width="744" height="66" rx="12" fill="#ffffff" stroke="#e7e5e4" stroke-width="1.2"/>
<text x="48" y="276" class="t-label">A 被释放，且 A 产生的在途消息全部消费</text>
<text x="355" y="276" class="t-label" fill="#0f766e">→　(1, 3, 0) 计数归零</text>
<text x="590" y="276" class="t-label" fill="#6d28d9">→　frontier = {(1, 3, 1)}</text>
<text x="48" y="302" class="t-micro">释放一个被覆盖的、更晚的 capability 不会推进 frontier；必须消掉支撑当前最小点的最后一份证据。</text>

</svg>
<figcaption class="fig-caption">capability 不直接“变成”一个 frontier 点。运行时先把 capability 和在途消息沿所有通向 P 的路径投影，维护候选时间的计数，再只保留偏序意义下的最小点。许多证据可以支撑同一个点，更晚且被覆盖的点不会出现在 frontier 中。</figcaption>
</figure>

在前面的 A → B 直线中，一个数字就能表示 B 的收件进度。二重循环中的 P 仍然是在判断“哪些时间已经收完”，只是一个未完成 pointstamp 沿不同路径可以在 P 推导出多个时间下界，“收到哪了”不一定能用一个点说清楚。

现在只留一份未完成工作：P@<code>(1, 2, 1)</code>。运行时从它推导出两类下界：

- 走长度为零的原地路径，它仍是 <code>(1, 2, 1)</code>：外层第 2 轮、内层第 1 轮及其以后还不能关闭；
- 走 <code>inner body → leave_inner → outer feedback → enter_inner</code> 回到 P，它变成 <code>(1, 3, 0)</code>：这份工作还可能启动外层第 3 轮，所以第 3 轮必须从内层第 0 轮开始等待。

这两个点在 <code>Product</code> 偏序下互不可比：<code>(1, 2, 1)</code> 的 outer 更小，<code>(1, 3, 0)</code> 的 inner 更小。第一个点挡住“当前外层轮次中从 inner=1 开始”的区域，却挡不住 <code>(1, 3, 0)</code>；第二个点正好补上“后续外层轮次从 inner=0 开始”的区域。因此 P 的 frontier 必须同时保留它们：

<pre><code>frontier(P) = {(1, 2, 1), (1, 3, 0)}</code></pre>

这给出了它不是字典序的直接证据。若用字典序，<code>(1, 2, 1) &lt; (1, 3, 0)</code>，后一个点会被删掉；当前 Timely 的 <code>Antichain::insert</code> 调用 <code>Product::less_equal</code>，所以两个点都会留下。

这里的“互不可比”只描述**同一个目标端口上的两个时间下界不能互相覆盖**，不表示两项工作没有因果关系。上面的两个 frontier 点甚至就是由同一份未完成工作沿两条路径推导出来的。这样一组两两无法用 <code>Product::less_equal</code> 覆盖的最小下界，叫作 **antichain（反链）**。

把数字换回前面讨论的 <code>(1, 2, 4)</code>，结论就很具体了：只要 P@<code>(1, 2, 4)</code> 这份工作还存在，P 的边界既要保留当前时间 <code>(1, 2, 4)</code>，也要保留它跨外层回边所推导出的 <code>(1, 3, 0)</code>。因此它不可能已经推进成只有 <code>{(1, 3, 1)}</code>。只有所有 outer=2 的未完成工作都消失，并且再也没有 capability 或在途消息能在 P 产生 <code>(1, 3, 0)</code>，这一方向才可能推进到 <code>(1, 3, 1)</code>。

反过来说，如果 P 的 frontier **只有** <code>{(1, 3, 0)}</code>，就已经承诺以后不会再到达任何 <code>(1, 2, x)</code>：因为 <code>(1, 3, 0)</code> 并不小于等于 <code>(1, 2, x)</code>。frontier 中必须有另一个 outer 不大于 2 的点，才能允许这种时间的消息继续到达。

反过来，如果两个下界满足 <code>a ≤ b</code>，frontier 里留下 <code>a</code> 就够了，因为 <code>a</code> 已经挡住了 <code>b</code> 所能挡住的全部目标时间。

先固定 epoch <code>e=1</code>，只画外层轮次 <code>o</code> 和内层轮次 <code>i</code>。下面紫色的两个点是 antichain 的成员；灰色点已经被某个紫色点逐坐标覆盖：

<figure class="fig-card" id="frontier-antichain">
<svg class="fig-svg" viewBox="0 0 760 470" role="img" aria-label="固定 epoch 1 后的二维乘积偏序网格。同一份未完成工作可沿不同路径在 P 推导出 (1,2,1) 与 (1,3,0) 两个下界；二者在乘积偏序下互不可比，所以共同组成 frontier 反链。">
<text x="28" y="28" class="t-title">antichain：同一份工作也可能推导出两个不可互相覆盖的下界</text>

<rect x="28" y="48" width="390" height="292" rx="13" fill="#fafaf9" stroke="#e7e5e4" stroke-width="1.2"/>
<text x="48" y="75" class="t-sub" font-weight="700">固定 e = 1；横轴是 outer，纵轴是 inner</text>

<g stroke="#e7e5e4" stroke-width="1">
<line x1="92" y1="104" x2="92" y2="300"/><line x1="162" y1="104" x2="162" y2="300"/><line x1="232" y1="104" x2="232" y2="300"/><line x1="302" y1="104" x2="302" y2="300"/><line x1="372" y1="104" x2="372" y2="300"/>
<line x1="92" y1="300" x2="372" y2="300"/><line x1="92" y1="251" x2="372" y2="251"/><line x1="92" y1="202" x2="372" y2="202"/><line x1="92" y1="153" x2="372" y2="153"/><line x1="92" y1="104" x2="372" y2="104"/>
</g>
<g stroke="#57534e" stroke-width="1.5">
<line x1="82" y1="300" x2="390" y2="300"/><line x1="92" y1="312" x2="92" y2="88"/>
</g>
<g class="t-micro" fill="#57534e">
<text x="92" y="319" text-anchor="middle">0</text><text x="162" y="319" text-anchor="middle">1</text><text x="232" y="319" text-anchor="middle">2</text><text x="302" y="319" text-anchor="middle">3</text><text x="372" y="319" text-anchor="middle">4</text>
<text x="74" y="304" text-anchor="end">0</text><text x="74" y="255" text-anchor="end">1</text><text x="74" y="206" text-anchor="end">2</text><text x="74" y="157" text-anchor="end">3</text><text x="74" y="108" text-anchor="end">4</text>
<text x="358" y="334">outer o →</text><text x="48" y="98">inner i ↑</text>
</g>

<path d="M 232 251 L 302 251 L 302 300 L 390 300" fill="none" stroke="#6d28d9" stroke-width="2" stroke-dasharray="6 5"/>
<circle cx="232" cy="251" r="7" fill="#6d28d9"/><circle cx="302" cy="300" r="7" fill="#6d28d9"/>
<text x="184" y="241" class="t-label" fill="#6d28d9">(1,2,1)</text>
<text x="312" y="289" class="t-label" fill="#6d28d9">(1,3,0)</text>

<g fill="#a8a29e">
<circle cx="232" cy="153" r="5"/><circle cx="302" cy="202" r="5"/><circle cx="372" cy="251" r="5"/>
</g>
<g class="t-micro" fill="#57534e">
<text x="242" y="147">(1,2,3)</text><text x="312" y="196">(1,3,2)</text><text x="326" y="241">(1,4,1)</text>
<text x="125" y="125">灰点被更早的紫点覆盖，不进入 frontier</text>
</g>

<rect x="442" y="48" width="290" height="130" rx="13" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.2"/>
<text x="460" y="76" class="t-title">能比较：删掉更晚的点</text>
<text x="460" y="108" class="t-label">(1,2,1) ≤ (1,2,3)</text>
<text x="460" y="134" class="t-label">(1,3,0) ≤ (1,3,2)</text>
<text x="460" y="160" class="t-sub">更早点已经表达“这些方向仍可能到来”</text>

<rect x="442" y="194" width="290" height="146" rx="13" fill="#ede9fe" stroke="#6d28d9" stroke-width="1.2"/>
<text x="460" y="222" class="t-title">不可比：两个方向都要保留</text>
<text x="460" y="254" class="t-label">(1,2,1)：outer 较早，inner 较晚</text>
<text x="460" y="280" class="t-label">(1,3,0)：outer 较晚，inner 较早</text>
<text x="587" y="318" text-anchor="middle" class="t-title" fill="#6d28d9">frontier = {(1,2,1), (1,3,0)}</text>

<rect x="28" y="366" width="704" height="78" rx="11" fill="#ffffff" stroke="#e7e5e4" stroke-width="1.2"/>
<text x="48" y="391" class="t-label">这不是两份工作清单，而是 P 上“还不能关闭”的两个区域起点</text>
<text x="48" y="415" class="t-micro">(1,2,1) 挡当前 outer 的后续 inner；(1,3,0) 挡后续 outer 从 inner=0 开始的全部时间。</text>
<text x="48" y="436" class="t-micro">支撑它们的原始 pointstamp 消失后，两条路径推导出的下界也随之撤销，frontier 才继续推进。</text>
</svg>
<figcaption class="fig-caption">antichain 的“anti”不是反向，而是这些时间下界无法用 <code>Product::less_equal</code> 连成一条链。这里 <code>(1,2,1)</code> 与 <code>(1,3,0)</code> 可以来自同一份 pointstamp 的两条路径；它们有因果联系，但在目标端口覆盖的时间区域不同，因此缺一不可。支撑原始 pointstamp 的最后一份 capability 或在途消息消失后，由它推导出的下界也会撤销。</figcaption>
</figure>

**扩展一下：什么时候 <code>(1,2,4)</code> 与 <code>(1,3,1)</code> 真会同时出现在 frontier 中？**

前面的循环入口 P 不会出现这种 frontier：P@<code>(1,2,4)</code> 自己还能跨外层回边产生 P@<code>(1,3,0)</code>，而 <code>(1,3,0)</code> 会覆盖 <code>(1,3,1)</code>。但换一个数据流位置，结论可能不同。

考虑下面的分叉—汇合图。较早的工作进入慢速旁路 A；它已经消费输入，只在 A 的输出端保留 <code>cap@(1,2,4)</code>。另一条支路继续绕循环，已经推进到 B@<code>(1,3,1)</code>。两条支路最后汇合到 Q，但从 A 的当前位置只能走向 Q，**没有边可以回到 feedback**：

<figure class="fig-card" id="frontier-side-branch">
<svg class="fig-svg" viewBox="0 0 780 330" role="img" aria-label="循环中的分叉汇合图。慢速旁路 A 持有时间 (1,2,4) 的 capability，但 A 没有通向 feedback 的路径；另一支路已沿循环推进到 (1,3,1)。两支路经 concat 汇入 Q，所以 Q 的 frontier 可以同时保留这两个不可比时间。">
<defs>
<marker id="side-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
<marker id="side-purple" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#6d28d9"/></marker>
</defs>
<text x="28" y="28" class="t-title">旁路与循环支路重新汇合：两个时间可以共同成为 Q 的 frontier</text>

<rect x="34" y="126" width="104" height="58" rx="10" fill="#ffffff" stroke="#57534e" stroke-width="1.2"/>
<text x="86" y="149" text-anchor="middle" class="t-label">此前的 fork</text>
<text x="86" y="171" text-anchor="middle" class="t-micro">原始消息已消费</text>

<rect x="190" y="62" width="246" height="86" rx="12" fill="#fafaf9" stroke="#57534e" stroke-width="1.2"/>
<text x="313" y="88" text-anchor="middle" class="t-title">慢速旁路 A</text>
<text x="313" y="112" text-anchor="middle" class="t-label" fill="#6d28d9">持有 cap@(1,2,4)</text>
<text x="313" y="136" text-anchor="middle" class="t-micro">A 只有通向汇合点的边，没有 feedback 路径</text>

<rect x="190" y="196" width="246" height="86" rx="12" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.2"/>
<text x="313" y="222" text-anchor="middle" class="t-title" fill="#0f766e">循环支路 B</text>
<text x="313" y="246" text-anchor="middle" class="t-micro">leave → outer feedback → enter</text>
<text x="313" y="270" text-anchor="middle" class="t-label" fill="#6d28d9">已经到达 (1,3,1)</text>

<rect x="502" y="126" width="112" height="58" rx="10" fill="#ffffff" stroke="#0f766e" stroke-width="1.3"/>
<text x="558" y="149" text-anchor="middle" class="t-label">concat M</text>
<text x="558" y="171" text-anchor="middle" class="t-micro">只汇合，不反馈</text>

<rect x="660" y="108" width="94" height="94" rx="12" fill="#ede9fe" stroke="#6d28d9" stroke-width="1.4"/>
<text x="707" y="134" text-anchor="middle" class="t-title" fill="#6d28d9">Q.input</text>
<text x="707" y="160" text-anchor="middle" class="t-micro">frontier</text>
<text x="707" y="181" text-anchor="middle" class="t-label">两个点</text>

<path d="M 138 148 L 166 148 L 166 105 L 186 105" fill="none" stroke="#57534e" stroke-width="1.8" marker-end="url(#side-teal)"/>
<path d="M 138 162 L 166 162 L 166 239 L 186 239" fill="none" stroke="#0f766e" stroke-width="1.8" marker-end="url(#side-teal)"/>
<path d="M 436 105 L 468 105 L 468 145 L 498 145" fill="none" stroke="#57534e" stroke-width="1.8" marker-end="url(#side-teal)"/>
<path d="M 436 239 L 468 239 L 468 166 L 498 166" fill="none" stroke="#0f766e" stroke-width="1.8" marker-end="url(#side-teal)"/>
<line x1="614" y1="155" x2="656" y2="155" stroke="#6d28d9" stroke-width="2" marker-end="url(#side-purple)"/>

<rect x="34" y="298" width="720" height="24" rx="8" fill="#ffffff" stroke="#e7e5e4"/>
<text x="394" y="315" text-anchor="middle" class="t-micro">关键不是 A 的时间较早，而是 A 当前所在的位置已经没有通向外层回边的路径。</text>
</svg>
<figcaption class="fig-caption">假设图中此刻只剩两份未完成证据：A 的 <code>cap@(1,2,4)</code> 和 B 的 <code>(1,3,1)</code> 消息或 capability。A 到 Q、B 到 Q 的路径都不改变时间；A 又无法绕外层回边给 Q 推导出 <code>(1,3,0)</code>。因此两个时间在 Q 上仍然互不可比，<code>frontier(Q) = {(1,2,4), (1,3,1)}</code>。</figcaption>
</figure>

这个例子说明，frontier 中能否同时出现两个时间，不能只看两个裸时间是否互不可比，还要看未完成工作**当前位于哪里**，以及从那里到目标端口还存在哪些路径。同一个时间 <code>(1,2,4)</code> 位于 P 时会压住 <code>(1,3,1)</code>；位于不能返回 feedback 的旁路 A 时，却可以和 <code>(1,3,1)</code> 一起留在 Q 的 frontier 中。

对 P 上的目标时间 <code>t</code>，判断“这个时间是否已经关闭”的条件于是非常直接：

<pre><code>若存在 f ∈ frontier(P)，满足 f ≤ t：仍有未来工作可能影响 t，答案尚未完整。
若不存在这样的 f：P 已经越过 t；该时间的输入已经关闭，相关结果可确认完整，状态可以回收。</code></pre>

这不是全局同步点。每个输入端口根据能到达自己的 capability 和在途消息维护各自的 frontier；跨 worker 的计数会被汇总，但 worker 不需要停下来互相等齐，不同算子也可以位于不同的逻辑进度。

**最后看不动点。** 对循环体来说，“这一瞬间队列为空”不能证明已经收敛，因为旧消息可能仍在路上，算子也可能还持有 capability。真正的数据不动点是：本轮输入不再产生新的差分；真正可被运行时确认的不动点则还要再多一步——所有可能回到循环入口的消息都已消费，所有能产生这类消息的 capability 都已释放，或把时间推进到了更晚。

这时，内层入口 P 不再有属于该轮次的候选时间，内层 frontier 会越过这些时间，必要时变成空反链；外层也以同样方式继续推进。当外层输出的 frontier 最终越过 epoch <code>e</code>，下游才得到一个可靠承诺：**这个 epoch 的循环结果以后不会再改。** <code>leave</code> 可以在计算过程中持续把差分输出到外层；frontier 越过 <code>e</code> 表示这些增量现在已经完整，而不是此刻才把整批结果一次性吐出。

所以三者的关系是：

<pre><code>时间戳与偏序：描述“旧工作还能影响哪里”
capability + 在途消息：记录“未来工作确实还存在”
frontier / antichain：把这些证据压缩成端口的完成边界
frontier 越过目标时间：把“没有新差分”确认为不动点</code></pre>

#### 4.2.5 iterate 算子：把回边封装成一次函数调用

有了嵌套时间戳，"循环"就可以从一种图结构变成一个普通算子。Differential Dataflow 的 `iterate` 正是这样做的。用它写 §3 的股权穿透查询：

```rust
// holds: (股东, 被持股公司) 静态表；start: 起始集合 {P}
let reach = start.iterate(|known| {
    known.join(&holds)           // 已知 ⋈ 持股表
         .map(|(_, owned)| owned)
         .concat(&known)         // 并入此前发现的全部公司
         .distinct()             // 去重：只保留第一次出现的
});
```

这段代码里看不到回边、时间戳和轮次，但它们都在算子内部原样存在：

1. 进入 `iterate` 时，输入消息的时间戳被压入 iteration 坐标，初值为 0；
2. 每一轮，循环体的输出沿回边送回输入，iteration 加一；
3. `distinct` 保证集合在有限轮后不再变化——到达不动点；
4. 循环结果会在计算过程中经 `leave` 持续输出；当相关 frontier 越过这些 iteration，并最终让外层 frontier 越过该 epoch 时，下游才确认此前收到的增量已经完整，不会再被修改。

对外部世界，整个 `iterate` 就是一个普通的映射算子：输入是某个 epoch 上的一批起始公司，输出是同一 epoch 上的穿透结果。**轮次被完整地封装在算子内部**——这是嵌套时间戳的回报：循环不再是图的特殊形状，而是一次可以组合的函数调用。§2 说计算图与函数表达式同构，到这里，递归函数也被收编了进来。

#### 4.2.6 同一条查询，按逻辑时间推演

回到 §3 的持股链，看 `iterate` 内部实际发生什么（下面是一种可能的交错顺序；异步执行中顺序本身不唯一）：

| 事件 | 消息（`@n` = 第 n 轮） | 结果 |
|---|---|---|
| 1 | P→甲@1, P→丙@1 | 甲、丙加入已知集合 |
| 2 | 甲→乙@2 | 乙加入，立即生效，不等任何人 |
| 3 | 甲→丙@2（重复） | 丙已知，去重算子就地吸收 |
| 4 | 丙→丁@2 | 丁加入，立刻继续传播 |
| 5 | 丁→戊@3 | 戊加入 |
| 6 | 乙→丙@3（重复） | 又一条重复消息，同样就地吸收 |
| 7 | 戊→甲@4（沿环回来） | 甲已知，吸收；不产生新的反馈数据 |
| 8 | 各算子 frontier 越过所有轮次 | 判定收敛，不需要多算一轮数据 |

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 720 260" role="img" aria-label="异步数据流：消息携带轮次标签在泳道间自由穿行，没有屏障，frontier 以虚线向前推进">
<defs>
<marker id="s42-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
</defs>
<g class="t-sub">
<text x="8" y="76">worker 1</text><text x="8" y="116">worker 2</text><text x="8" y="156">worker 3</text><text x="8" y="196">worker 4</text>
</g>
<g stroke="#0f766e" stroke-width="2" fill="none">
<line x1="80" y1="68" x2="200" y2="68" marker-end="url(#s42-teal)"/>
<line x1="120" y1="68" x2="150" y2="108" marker-end="url(#s42-teal)"/>
<line x1="200" y1="108" x2="330" y2="108" marker-end="url(#s42-teal)"/>
<line x1="260" y1="108" x2="290" y2="148" marker-end="url(#s42-teal)"/>
<line x1="90" y1="148" x2="210" y2="148" marker-end="url(#s42-teal)"/>
<line x1="330" y1="148" x2="430" y2="148" marker-end="url(#s42-teal)"/>
<line x1="150" y1="188" x2="280" y2="188" marker-end="url(#s42-teal)"/>
<line x1="380" y1="68" x2="480" y2="68" marker-end="url(#s42-teal)"/>
<line x1="460" y1="108" x2="560" y2="108" marker-end="url(#s42-teal)"/>
</g>
<g class="t-micro" fill="#0f766e" font-weight="700">
<text x="135" y="60">iter=1</text><text x="255" y="100">iter=2</text><text x="140" y="140">iter=1</text>
<text x="205" y="180">iter=2</text><text x="425" y="60">iter=3</text><text x="375" y="140">iter=3</text>
<text x="505" y="100">iter=4</text>
</g>
<line x1="320" y1="44" x2="320" y2="212" stroke="#6d28d9" stroke-width="2" stroke-dasharray="8 5"/>
<text x="320" y="228" text-anchor="middle" class="t-micro" fill="#6d28d9" font-weight="700">frontier 推进中</text>
<text x="560" y="228" text-anchor="middle" class="t-sub">无屏障：不同轮次的消息同时流动</text>
</svg>
<figcaption class="fig-caption">异步数据流：消息自带轮次标签，第 3、4 轮的消息不必等前两轮全部走完。frontier 是一条持续推进的时间下界，不是屏障。</figcaption>
</figure>

对比同步轮次，三个结构性差异：

1. **轮次可以重叠**。同步模型里第 k+1 轮必须等第 k 轮全部结束；Timely 里第 2 轮的消息不必等第 1 轮全部走完，不同轮次的计算在系统里同时流动。这是同步路线结构性拿不到的好处。
2. **重复消息就地吸收**。甲→丙、乙→丙、戊→甲这些重复发现到达时经过去重算子即被丢弃，不会拖住一整轮。
3. **收敛靠数学判定**。frontier 越过所有轮次，循环封闭，不需要空转一轮数据。

为了保持诚实，两点必须说清楚。第一，**重复消息的代价不是零**：它照样被产生、传输、查询一次去重状态，省掉的是"拖住整轮"，不是消息本身的开销。第二，**"不需要空转"省的是数据轮次，不是通信**——frontier 的推进本身需要各节点持续交换进度消息。异步模型没有消除同步，而是把"每轮一次全局屏障"换成了一套常驻的、细粒度的进度协议。

代价还有编程门槛：每条消息都要正确携带时间戳，每个算子都要参与进度追踪（在 Timely 里体现为 capability 的管理），这是同步模型的程序员不需要操心的。

**追问一：iterate 会攒够一批消息再处理吗？** 不会。数据面上消息逐条立即处理——物理时间轴里 `戊@(3,3)` 抢在 `(3,2)` 的最后一条消息之前流动，就是证据。唯一会“等”的是 frontier 这条控制通道，而它只回答“哪个逻辑时间已经关闭、相关状态可以回收”，不拦截数据。

**追问二：那重复消息的代价怎么压低？** 靠合并（consolidation）。Differential 的每条更新是（数据， 时间， 权重）三元组，系统会随手把同一（数据， 时间）的更新合并：权重相加，抵消为零的直接删除。效果立竿见影——同一轮里先 +1 又 −1，合并后等于什么都没发生，下游零工作量；同一个 key 在同一时间被两次 +1，合并成 +2，distinct 只需输出一次。注意这是"随到随合并"，不是"攒够了再发"：合并不引入任何等待，只是把废更新消灭在传播途中。worker 之间的网络打包同理，是吞吐优化，对语义透明。

**追问三：有界、无界与“没有数据”的进度有什么不同？**

到这里再看**有界计算**与**无界输入**，区别会更清楚。MPI、Pregel、Giraph、GraphX 和 Gelly 的同步迭代都以“本轮工作能够收完”为前提：一轮中只有有限的消息或分区，屏障等到它们全部完成，再开始下一轮。**不能把一整条永不结束的无界流直接当成一次同步 iteration**：输入永远收不完，第一轮的全局屏障也就永远无法关闭。若要在持续输入上复用这种语义，必须先把输入切成窗口、微批或其他能够关闭的逻辑区间，再分别讨论每个区间的迭代。

无界流上的 join 也有同样的问题。两条无界流可以一边到数据一边增量地产生匹配结果；但如果 join 没有窗口或时间范围，任意一条旧记录都可能与很久以后到达的新记录匹配，系统既无法宣布某个时间范围的答案已经完整，也无法安全删除全部旧状态。在 Flink 的事件时间计算中，watermark 与窗口或 interval join 的时间约束配合，用来说明事件时间已经推进到哪里，从而触发窗口结果并回收不再可能匹配的状态。**watermark 不是让 join 才能运行的开关，而是让系统能够关闭时间范围、控制状态生命周期的进度依据。**

Timely 不为有限任务与持续任务准备两套执行引擎，但这不表示“一整条无界流也能最终迭代完成”。有限输入停止后，frontier 会不断推进，最终可以变成空反链；持续输入的整体 frontier 通常不会结束，只会随着输入进度前移。长期运行的数据流图可以包含 iterative scope，完成判定则落在一个个能够关闭的逻辑时间上：epoch 5 的输入 frontier 越过 5 后，系统可以继续确认 epoch 5 在循环内是否到达不动点，同时 epoch 6、7 仍可进入图中。

这里最容易误解的是“没有数据”。假设 epoch 5 根本没有记录，source 不需要伪造一条空消息；它只要把输入 capability 从 5 推进到 6，就已经明确承诺“以后不会再发送 epoch 5 的数据”。frontier 随之越过 5，Timely 仍然可以确认 epoch 5 的完整答案——它可能是空集，也可能是由此前状态和其他输入决定的结果。真正让 epoch 5 永远无法完成的，不是“这一段没有数据”，而是 source 既不发送数据，也一直保留 <code>cap@5</code>，使运行时无法排除未来还会出现 epoch 5。

因此，Timely 统一的是有限任务与持续任务的**执行和进度表达**：数据都按到达顺序增量计算，是否完整都由 capability 与 frontier 证明；它没有凭空为一整条无界输入制造终点。到第二篇，<code>(data, time, diff)</code> 三元组会成为主角，无界 join 的状态保存与清理、更新合并也会继续展开。

### 4.3 两种路线对照

| 维度 | 同步轮次（BSP） | 逻辑时间（Timely） |
|---|---|---|
| 代表实现 | MPI、Pregel、Giraph、GraphX、Gelly | Timely / Differential Dataflow |
| 时间在哪 | 系统全局状态（轮次编号，隐式、全序） | 每条消息携带（显式时间戳，偏序） |
| 进度判定 | 轮末屏障 + 全局检查 | frontier 下界，逐算子推进 |
| 轮次关系 | 严格先后，不可重叠 | 可重叠，乱序到达是常态 |
| 重复消息 | 作废已消耗的一轮算力 | 到达即被去重吸收 |
| 收敛判据 | 任意全局聚合（如 max\|Δ\|<ε） | 受单调性约束（时间不再前进） |
| 长处 | 语义简单；每轮末全局一致快照 | 无整轮等待；无需空转确认 |
| 短板 | straggler；整轮物化；空转轮 | 进度协议自身开销；编程门槛 |

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答的问题是"循环里的时间记在哪里"。<strong>时间要么记在系统里，要么记在数据里。</strong>同步轮次用全局等待换来语义的简单；逻辑时间用更精细的簿记换来轮次的重叠。两条路线表达的是同一个循环，差别在于谁来记录"现在"。</p>
</div>

### 4.4 用 SQL 说出来：相关子查询与递归

两条路线最后都要落到用户写的查询上。先看两种 SQL 形态：一种是非等值相关子查询，它让每条外层记录携带自己的搜索条件；另一种是 SQL 的递归模型，它让一个集合的定义反复作用于上一轮结果。它们都能用 §4.2 的 scope 表达，但表达的是两种不同的工作。

#### 4.4.1 非等值相关子查询：截止时刻之前的最近事件

考虑一条“截止时刻之前最近发生了什么”的查询：

```sql
SELECT u.id,
       (
         SELECT e.value
         FROM events e
         WHERE e.user_id = u.id
           AND e.ts <= u.cutoff
         ORDER BY e.ts DESC
         LIMIT 1
       ) AS last_value
FROM users u;
```

外层每来一条 `u`，内层都要回答同一个问题：这个用户在 `cutoff` 之前最近的一条事件是什么。`user_id` 是等值相关条件，`ts <= cutoff` 是非等值范围条件；真正让执行复杂起来的，是每个外层行都有自己的 `cutoff`，还要在范围内取按时间倒序的第一条。

这里需要先澄清一个容易误会的说法：Flink SQL 并不是只能把非等值条件交给 theta join。它的 `SubQueryDecorrelator` 支持等值和非等值相关条件，非等值 `EXISTS` 通常也可以改写为 semi join。

真正困难的是这条**相关标量 Top-1**。Flink 把 Correlate 改写成 Rank/Top-N 时，要求相关条件能形成等值分组；`e.user_id = u.id` 满足这一要求，`e.ts <= u.cutoff` 却是每个外层记录各不相同的范围条件。由于完整模式不满足 Rank 改写的前提，计划可能保留 Correlate，也就是每条外层记录驱动一次内层扫描、排序和 `LIMIT 1`。

流式执行还受另一个约束：regular join 至少需要一个等值键来分区状态，其他非等值条件只能在等值键匹配之后再检查。这里的问题是优化器和运行时可能落入更贵的执行路径，而不是 SQL 表达不了这类查询。

Timely 有另一种实现思路：不把所有外层记录和内层事件先展开成候选集合，而是把每个外层条件封装进一个嵌套 scope。`events` 按 `(user_id, 时间桶)` 维护成常驻 arrangement；一条 `(u.id, u.cutoff)` 进入 scope 时带上 `(epoch, 0)`，先探测 `cutoff` 所在的时间桶。命中就取出该桶内最新事件并离开；没命中就把桶编号减一，沿 feedback 进入 `(epoch, 1)`，继续探测更早的桶。离开 scope 时，iteration 坐标被弹出，输出仍属于外层 epoch。

<figure class="fig-card fig-card--dense">
<svg class="fig-svg" viewBox="0 0 760 440" role="img" aria-label="相关标量 Top-1 查询在 Timely 的嵌套 scope 中按时间桶逐轮探测：外层记录进入 scope，未命中沿 feedback 换到更早桶，命中后取出 Top-1 并离开 scope">
<defs>
<marker id="asof-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
<marker id="asof-idle" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#a8a29e"/></marker>
</defs>
<text x="30" y="30" class="t-title">外层输入 → 嵌套搜索 scope → 输出</text>
<rect x="30" y="58" width="150" height="56" rx="12" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.5"/>
<text x="105" y="80" text-anchor="middle" class="t-label">u = 42</text><text x="105" y="100" text-anchor="middle" class="t-sub">cutoff = 10:37 · @e</text>
<line x1="184" y1="86" x2="250" y2="86" stroke="#a8a29e" stroke-width="2" marker-end="url(#asof-idle)"/>
<text x="217" y="76" text-anchor="middle" class="t-micro">enter</text>
<rect x="250" y="48" width="480" height="300" rx="18" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.6"/>
<text x="274" y="76" class="t-title" fill="#0f766e">Correlated Search Scope · (e, i)</text>
<text x="694" y="76" text-anchor="end" class="t-sub" fill="#0f766e">scope 内的时间比外层多一个坐标</text>
<g>
<rect x="276" y="104" width="106" height="42" rx="10" fill="#ffffff" stroke="#a8a29e"/><text x="329" y="129" text-anchor="middle" class="t-label">probe i=0</text>
<rect x="414" y="104" width="106" height="42" rx="10" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="467" y="129" text-anchor="middle" class="t-label">probe i=1</text>
<rect x="552" y="104" width="132" height="42" rx="10" fill="#0f766e"/><text x="618" y="129" text-anchor="middle" class="t-white">Top-1 / leave</text>
<line x1="382" y1="125" x2="410" y2="125" stroke="#a8a29e" stroke-width="2" stroke-dasharray="5 4" marker-end="url(#asof-idle)"/>
<line x1="520" y1="125" x2="548" y2="125" stroke="#0f766e" stroke-width="2.4" marker-end="url(#asof-arrow)"/>
<path d="M 329 148 C 322 178, 350 190, 414 158" stroke="#0f766e" stroke-width="2" stroke-dasharray="6 5" fill="none" marker-end="url(#asof-arrow)"/>
<text x="372" y="186" text-anchor="middle" class="t-micro" fill="#0f766e">未命中：bucket - 1，(e,i) → (e,i+1)</text>
</g>
<g>
<text x="306" y="226" class="t-sub">user 42 的 events arrangement</text>
<rect x="276" y="244" width="128" height="42" rx="8" fill="#ffffff" stroke="#a8a29e"/><text x="340" y="264" text-anchor="middle" class="t-label">10:30–10:39</text><text x="340" y="280" text-anchor="middle" class="t-micro">i=0 · 无满足行</text>
<rect x="414" y="244" width="128" height="42" rx="8" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="478" y="264" text-anchor="middle" class="t-label">10:20–10:29</text><text x="478" y="280" text-anchor="middle" class="t-micro" fill="#0f766e">i=1 · 命中 10:26</text>
<rect x="552" y="244" width="128" height="42" rx="8" fill="#ffffff" stroke="#d6d3d1" stroke-dasharray="4 3"/><text x="616" y="264" text-anchor="middle" class="t-label">10:10–10:19</text><text x="616" y="280" text-anchor="middle" class="t-micro">不再探测</text>
<line x1="329" y1="148" x2="329" y2="238" stroke="#a8a29e" stroke-width="1.5" stroke-dasharray="3 4" marker-end="url(#asof-idle)"/>
<line x1="467" y1="148" x2="467" y2="238" stroke="#0f766e" stroke-width="2" marker-end="url(#asof-arrow)"/>
</g>
<g>
<rect x="590" y="302" width="118" height="34" rx="9" fill="#ffffff" stroke="#0f766e" stroke-width="1.4"/><text x="649" y="323" text-anchor="middle" class="t-label">弹出坐标 → @e</text>
</g>
<line x1="618" y1="148" x2="618" y2="372" stroke="#0f766e" stroke-width="2.4" marker-end="url(#asof-arrow)"/>
<rect x="548" y="374" width="150" height="44" rx="11" fill="#ffffff" stroke="#0f766e" stroke-width="1.5"/><text x="623" y="394" text-anchor="middle" class="t-label">last_value</text><text x="623" y="410" text-anchor="middle" class="t-micro">(42, 10:26) · @e</text>
<g class="t-sub">
<circle cx="42" cy="378" r="4" fill="#a8a29e"/><text x="54" y="382">enter：外层时间 e → scope 内 (e,0)</text>
<circle cx="42" cy="404" r="4" fill="#0f766e"/><text x="54" y="408">feedback：只携带还没完成的搜索状态</text>
<circle cx="42" cy="430" r="4" fill="#0f766e"/><text x="54" y="434">leave：命中即退出，不扫更早的桶</text>
</g>
</svg>
<figcaption class="fig-caption">相关条件不必先展开成完整候选集合。外层记录带着自己的 cutoff 进入 scope，按时间桶逐轮探测；命中后立刻弹出 iteration 坐标，未命中才把“继续找更早桶”的状态沿 feedback 送回。不同外层记录可以停在不同 iteration。</figcaption>
</figure>

这张图的收益来自三个物理事实：候选空间按时间和用户预先组织好了，搜索状态可以复用，命中后能提前结束。它不是 SQL 语义强制的：有合适的 as-of join 或范围索引时，直接范围连接可能更快；如果几乎每条外层记录都要扫到最后一个桶，逐桶反馈也帮不上忙。这里要说的是，Timely 允许把这种**逐步搜索过程**写进一个可组合的 scope，而不是只能把问题压成一次巨大的 join。

#### 4.4.2 SQL 的递归模型：WITH RECURSIVE

第二种 SQL 形态是 §3 一直使用的股权穿透。标准写法如下：

```sql
WITH RECURSIVE reach(company) AS (
  SELECT 'P'                                   -- 锚成员：起点
  UNION                                        -- 去重：重复发现被吸收
  SELECT h.owned                               -- 递归成员：拿上一轮的发现再查一轮
  FROM reach r JOIN holds h ON r.company = h.holder
)
SELECT company FROM reach;
```

它和 §3 的循环同构：锚成员是初始的 Δ 集合；递归成员里的 JOIN 是每轮拿新结果继续扩展；UNION 的去重就是 distinct，也是终止性。SQL 标准还规定了半朴素求值：递归成员只引用上一轮产生的行，所以每轮的“Δ ⋈ holds”不是某个引擎的临时优化。

以 OceanBase 为例，真实执行是教科书式的迭代驱动：先执行锚成员并物化第一张工作表；每一轮拿上一轮工作表去 join `holds`，去重后物化下一张工作表；某一轮没有新行，迭代结束。每一轮内部可以走 PX 并行，但**轮与轮之间是串行的**——第 k+1 轮必须等第 k 轮结果全部物化才能开始。这正是 §4.1 同步轮次在 SQL 引擎里的样子。

顺带一个历史事实：纯关系代数（select / project / join / union）被证明表达不了传递闭包（Aho & Ullman, POPL 1979），所以 SQL:1999 专门把递归写进了标准。循环不是语法糖——没有它，股权穿透这类查询在 SQL 里根本说不出来。

### 4.5 两种执行模型：火山与数据流

火山模型由控制流驱动数据流，Timely 由数据流驱动计算。火山模型里，父算子不断调用子算子的 `next()`，执行顺序由调用栈规定；Timely 的查询图描述的是记录怎么变换、流向哪里，而不是规定全局的“第 k 步做什么”。一条记录或一个搜索状态到达某个算子，负责该算子的 worker 就执行对应的任务。

把 §4.4 的两条 SQL 放进来，差别会更具体。对相关标量 Top-1，Volcano 可以把外层记录作为参数，驱动内层的 Sort + Limit：每条外层记录绑定一次 `cutoff`，打开或复用一次内层计划，取到结果后关闭。OceanBase 的优化器可能用半连接、窗口或其他形式改写它，但调用顺序仍由算子树和 driver 决定。Timely 则把查询翻译成长期存在的操作位置：外层条件进入 scope，内层 arrangement 常驻，没有命中的搜索状态沿 feedback 继续移动，命中的记录直接离开。不是有人命令“现在检查下一个桶”，而是“下一条待搜索记录”流到了那个算子。

对 SQL 递归，差别更明显。Volcano 的算子树本身不能有环：如果子节点沿树指回父节点，`next()` 调用就会无限嵌套。因此 Recursive Union 的 driver 必须放在树外，保存工作表、结束一轮、再重新打开递归子树。Timely 的 feedback 本来就是图中的一条数据边，新产生的 Δ 记录带着更晚的 iteration 坐标回到下一轮；相关搜索反馈的是未完成状态，递归查询反馈的是新发现的数据，使用的是同一类机制。

<figure class="fig-card fig-card--dense">
<svg class="fig-svg" viewBox="0 0 760 490" role="img" aria-label="左侧火山模型中控制流沿调用栈驱动相关子查询和递归工作表，driver 在树外循环；右侧 Timely 中不同 iteration 的记录沿数据流图触发算子，同时各 worker 持续汇报进度并推动 frontier">
<defs>
<marker id="model-orange" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#9a3412"/></marker>
<marker id="model-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
</defs>
<text x="30" y="32" class="t-title">Volcano / control flow</text>
<text x="414" y="32" class="t-title" fill="#0f766e">Timely / data flow</text>
<line x1="380" y1="20" x2="380" y2="458" stroke="#e8e0d4" stroke-dasharray="2 4"/>
<g>
<rect x="90" y="56" width="180" height="42" rx="10" fill="#ffedd5" stroke="#9a3412" stroke-width="1.5"/><text x="180" y="82" text-anchor="middle" class="t-label" font-weight="600">Project</text>
<rect x="90" y="128" width="180" height="42" rx="10" fill="#fff4ed" stroke="#9a3412" stroke-width="1.5"/><text x="180" y="154" text-anchor="middle" class="t-label">Correlate / Recursive Union</text>
<rect x="90" y="200" width="180" height="42" rx="10" fill="#ffffff" stroke="#57534e"/><text x="180" y="226" text-anchor="middle" class="t-label">Sort + Limit / work table</text>
<line x1="180" y1="98" x2="180" y2="124" stroke="#9a3412" stroke-width="2.2" marker-end="url(#model-orange)"/>
<line x1="180" y1="170" x2="180" y2="196" stroke="#9a3412" stroke-width="2.2" marker-end="url(#model-orange)"/>
<circle cx="64" cy="116" r="11" fill="#ffedd5" stroke="#9a3412"/><text x="64" y="120" text-anchor="middle" class="t-micro" fill="#9a3412">1</text>
<text x="82" y="120" class="t-micro" fill="#9a3412">next()</text>
<circle cx="64" cy="188" r="11" fill="#ffedd5" stroke="#9a3412"/><text x="64" y="192" text-anchor="middle" class="t-micro" fill="#9a3412">2</text>
<text x="82" y="192" class="t-micro" fill="#9a3412">bind cutoff</text>
<path d="M 278 220 C 342 252, 334 82, 270 78" stroke="#9a3412" stroke-width="2.2" stroke-dasharray="7 5" fill="none" marker-end="url(#model-orange)"/>
<circle cx="308" cy="112" r="11" fill="#ffedd5" stroke="#9a3412"/><text x="308" y="116" text-anchor="middle" class="t-micro" fill="#9a3412">3</text>
<text x="318" y="144" text-anchor="middle" class="t-micro" fill="#9a3412">reopen / next round</text>
<rect x="62" y="286" width="236" height="42" rx="10" fill="#ffedd5" stroke="#9a3412"/><text x="180" y="312" text-anchor="middle" class="t-label">driver 在树外控制“再跑一轮”</text>
<text x="180" y="366" text-anchor="middle" class="t-sub">线程按调用栈决定下一步</text>
<text x="180" y="446" text-anchor="middle" class="t-title">下一步调用谁？</text>
</g>
<g>
<rect x="414" y="64" width="96" height="38" rx="10" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.5"/><text x="462" y="88" text-anchor="middle" class="t-label">enter / Δ</text>
<rect x="548" y="64" width="118" height="38" rx="10" fill="#ffffff" stroke="#57534e"/><text x="607" y="88" text-anchor="middle" class="t-label">probe / join</text>
<rect x="548" y="152" width="118" height="38" rx="10" fill="#ccfbf1" stroke="#0f766e" stroke-width="1.5"/><text x="607" y="176" text-anchor="middle" class="t-label">output / distinct</text>
<rect x="414" y="152" width="96" height="38" rx="10" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.5"/><text x="462" y="176" text-anchor="middle" class="t-label">feedback</text>
<line x1="510" y1="83" x2="544" y2="83" stroke="#0f766e" stroke-width="2.2" marker-end="url(#model-teal)"/>
<line x1="607" y1="102" x2="607" y2="148" stroke="#0f766e" stroke-width="2.2" marker-end="url(#model-teal)"/>
<line x1="548" y1="171" x2="514" y2="171" stroke="#0f766e" stroke-width="2.2" marker-end="url(#model-teal)"/>
<path d="M 462 152 C 448 124, 452 104, 462 102" stroke="#0f766e" stroke-width="2.2" stroke-dasharray="6 5" fill="none" marker-end="url(#model-teal)"/>
<text x="536" y="126" text-anchor="middle" class="t-micro" fill="#0f766e">未完成状态</text>
<g>
<rect x="674" y="58" width="44" height="24" rx="12" fill="#ccfbf1" stroke="#0f766e"/><text x="696" y="74" text-anchor="middle" class="t-micro">(e,0)</text>
<rect x="674" y="106" width="44" height="24" rx="12" fill="#ffffff" stroke="#a8a29e"/><text x="696" y="122" text-anchor="middle" class="t-micro">(e,1)</text>
<rect x="674" y="154" width="44" height="24" rx="12" fill="#ccfbf1" stroke="#0f766e"/><text x="696" y="170" text-anchor="middle" class="t-micro">(e,2)</text>
</g>
<rect x="430" y="224" width="272" height="42" rx="10" fill="#f0fdfa" stroke="#0f766e"/><text x="566" y="250" text-anchor="middle" class="t-label">记录到达哪个算子，哪个算子就工作</text>
<rect x="408" y="288" width="316" height="112" rx="12" fill="#ffffff" stroke="#e8e0d4"/>
<text x="426" y="310" class="t-label" fill="#0f766e" font-weight="600">progress tracker 持续计算 frontier</text>
<text x="426" y="328" class="t-micro">数据面处理记录；进度面同时汇总 capability 与在途消息</text>
<line x1="438" y1="360" x2="694" y2="360" stroke="#d6d3d1" stroke-width="1.5"/>
<circle cx="458" cy="360" r="7" fill="#ccfbf1" stroke="#0f766e"/><text x="458" y="383" text-anchor="middle" class="t-micro">(e,0) 封闭</text>
<circle cx="558" cy="360" r="7" fill="#0f766e"/><text x="558" y="383" text-anchor="middle" class="t-micro" fill="#0f766e">frontier (e,1)</text>
<circle cx="662" cy="360" r="7" fill="#ffffff" stroke="#a8a29e"/><text x="662" y="383" text-anchor="middle" class="t-micro">(e,2) 在途</text>
<line x1="558" y1="340" x2="558" y2="372" stroke="#0f766e" stroke-width="2" stroke-dasharray="5 4"/>
<path d="M 570 344 C 592 330, 620 330, 642 344" stroke="#0f766e" stroke-width="1.5" fill="none" marker-end="url(#model-teal)"/>
<text x="606" y="342" text-anchor="middle" class="t-micro" fill="#0f766e">消息完成就继续推进</text>
<text x="566" y="446" text-anchor="middle" class="t-title" fill="#0f766e">这条数据变成什么、流向哪里？</text>
</g>
</svg>
<figcaption class="fig-caption">火山模型用调用顺序驱动数据：driver 决定何时绑定参数、何时重开子树。Timely 用数据驱动计算：不同 iteration 的记录在图中同时流动，算子处理数据的同时，progress tracker 持续汇总 capability 与在途消息并推进 frontier。相关搜索和 SQL 递归都可以使用同一条 feedback 数据边。</figcaption>
</figure>

| 维度 | 火山模型 | Timely Dataflow |
|---|---|---|
| 驱动力 | 上层算子调用下层 `next()` | 记录或更新到达算子 |
| 工作单元 | 一次调用、一棵子树、一轮工作表 | 一条记录、一个搜索状态、一次增量 |
| 相关 Top-1 | 外层行绑定参数，驱动内层 Sort + Limit | 外层行进入 scope，未完成状态逐桶反馈 |
| SQL 递归 | 树外 driver 物化工作表并启动下一轮 | 新 Δ 记录沿图内 feedback 返回 |
| 完成判定 | 子树关闭、工作表为空、driver 结束 | frontier 越过相关时间点，输出可确认完整，相关状态可回收 |

这并不意味着 Timely 没有控制：运行时仍然调度算子、管理线程、追踪 frontier。区别在于，这些控制是执行系统的基础设施，不是查询逻辑本身。火山模型把“先做什么、再做什么”写进调用关系；数据流模型把“数据如何变化”写进图，剩下的事交给数据到达来触发。

图中右下角不是一次偶尔触发的“收尾检查”。每个 worker 在发送消息、消费消息、保留或释放 capability 时，都会同步更新自己的进度；这些局部更新被持续汇总成 frontier。于是数据面可能正在处理 `(e,2)` 的记录，进度面同时确认 `(e,0)` 已经封闭，并判断 `(e,1)` 之后还可能出现什么。iteration 的计算和 frontier 的计算始终并行发生，系统不需要停下数据流，再专门组织一次全局检查。

<div class="callout callout--insight">
<p><strong>归因</strong>：控制流告诉线程“下一步调用谁”，数据流声明“这条数据变成什么、流向哪里”。前者把计算组织成一次调用；后者把计算组织成一组持续存在、对数据变化作出反应的算子。</p>
</div>

到目前为止，两种模型讨论的都是怎么把一批输入算完。输入一批接一批到来时，循环外面又多了一层时间边界：这一批从哪里开始、到哪里结束，正是下一节 epoch 要回答的问题。

## 5. 从一次计算到流计算

### 5.1 epoch：给持续输入一个逻辑边界

到目前为止，所有计算的输入都是一批已经到齐的数据。真实系统里输入不会停：持股表每天在变，订单每分钟在来。系统最自然的做法，是把连续的输入切成一段一段——第一批是 epoch 0，下一批是 epoch 1。

注意 **epoch 是逻辑边界，不是物理批次**。系统不需要把 epoch 3 的数据攒齐才开始处理 epoch 4 的数据；epoch 只是写在消息上的最外层时间坐标。有了 §4 的偏序，不同 epoch 的消息互不等待：`(3, 2)` 和 `(4, 1)` 不可比，第 4 批的第 1 轮不必等第 3 批收尾。

至此，整个时间结构成型：最外层的 epoch 区分"第几批输入"，内层的 iteration 区分"这批输入内部迭代到第几轮"。流计算不是什么新物种——它就是**一个 epoch 接一个 epoch 的并行计算**，每一批内部可能还套着本文讲的循环。

跨 epoch 会发生什么？第 2 批查询往往要用到第 1 批留下的东西：累计的已知集合、上次的中间结果。**跨 epoch 保存和更新的结果就是状态**——这是第二篇的主题。而 epoch 的边界在分布式环境下怎么划定、数据迟到怎么办，是第三篇 watermark 和 checkpoint 要回答的。

### 5.2 首尾呼应：任意长度数组的 Hillis-Steele

回到 §1 开头那 8 个数。当时我们把前缀和画成一张静态图：3 轮，跨度 1、2、4，图的形状由"8"这个数决定。如果数组长度事先不知道呢？这正是静态图画不出来、而 loop 刚好多出来的那种能力。

用 `iterate` 写一遍（示意伪代码，沿用 Differential 的风格）：

```rust
// 记录：(位置 i, 当前和 v, 当前跨度 s)，初始 s = 1；数组长度未知
let prefix = rows.iterate(|cur| {
    let advanced =
        cur.join(&cur)                  // 配对：位置 j == i - s
           .map(|(i, v, s), (_j, w, _)| (i, v + w, 2 * s));
    cur.concat(&advanced)               // 旧值保留，新值叠加
       .consolidate()                   // 合并同一 (位置, 时间) 的废更新
});
// s 超过最大下标后，join 不再产生新值 → 不动点 → frontier 封闭本 epoch
```

每个元素随身携带当前跨度：第 0 轮跨度 1，第 1 轮跨度 2，第 k 轮跨度 2ᵏ。当跨度超过数组里最大的下标，join 不再产生新值——不动点到达，frontier 越过所有 iteration，循环封闭，前缀和作为这个 epoch 上的普通输出离开 `iterate`。

对照 §1，三件事变了：

1. **层数不再出现在程序里**。`⌈log₂ n⌉` 由数据决定，运行时才展开——正是 §4 那张"逻辑分层、物理不分层"的图。
2. **时间戳参与了计算**。跨度等于 2 的 iteration 次方——iteration 坐标不只是簿记，它直接参数化了循环体的行为。
3. **合并有了用武之地**。每轮 `concat` 保留旧值又写入新值，`consolidate` 把同一（位置， 时间）的废更新消灭在传播途中——§4 末尾那两条追问的机制，在这里干活。

而如果每分钟都来一批新数组，它们各占一个 epoch，各自跑各自的迭代，互不等待。

### 5.3 本篇小结

- **并行计算的极限**：work 决定总账（单机时间 T₁），span 决定下限（无限机器时间 T∞）。机器摊得薄 work，摊不动 span。
- **依赖连成 DAG**：无环就是"算一次就完"的数学说法；DAG 与纯函数表达式同构，静态图的形状编译期确定。
- **循环 = 回边 + 进度判定**：静态展开注定要么浪费要么算错；distinct 不只是性能优化，它是终止性本身。
- **两条表达路线**：时间记在系统里（屏障，BSP），或记在数据里（逻辑时间戳，Timely）。偏序允许"不可比"，轮次和批次因此可以重叠。
- **iterate 把轮次封装进算子**：进入压坐标、回边加一、不动点弹出；epoch 是最外层坐标——流计算就是一个 epoch 接一个 epoch 的并行计算。

第二篇从状态开始：跨 epoch 的记忆从哪来，以及 (data, time, diff) 如何把插入和删除统一成带符号的更新。

## 延伸阅读

- Gilles Kahn, "The Semantics of a Simple Language for Parallel Programming" (IFIP Congress 1974)：进程网络的原始定义
- R. P. Brent, "The Parallel Evaluation of General Arithmetic Expressions" (JACM 1974)：§1 Brent 定理的出处
- Goetz Graefe, "Volcano—An Extensible and Parallel Query Evaluation System" (IEEE TKDE 1994)：火山模型的定型
- Goetz Graefe, "Encapsulation of Parallelism in the Volcano Query Processing System" (SIGMOD 1990)：exchange 算子
- Tucker, Maier, Sheard, Fegaras, "Exploiting Punctuation Semantics in Continuous Data Streams" (IEEE TKDE 2003)：标点消息（punctuation）
- Hueske et al., "Opening the Black Boxes in Data Flow Optimization" (PVLDB 2012)：流式 pipeline 边界的形式化，Stratosphere/Flink
- Naiad: A Timely Dataflow System (SOSP'13) §2–3：嵌套时间戳、progress tracking、frontier
- Differential Dataflow (CIDR'13)：`iterate` 与 semi-naïve 求值
- Malewicz et al., "Pregel" (SIGMOD 2010)：superstep 同步迭代
- Valiant, "A Bridging Model for Parallel Computation" (1990)：BSP 模型
- Blelloch, "Prefix Sums and Their Applications"：扫描算法的深度与工作量权衡
- Aho & Ullman, "Universality of Data Retrieval Languages" (POPL 1979)：关系代数表达不了传递闭包
- Gray et al., "Data Cube" (1997)：聚合算子的 distributive / algebraic / holistic 分类
- Finkelstein et al., "Expressing Recursive Queries in SQL" (X3H2-96-075)：递归查询进入 SQL 标准的来龙去脉
