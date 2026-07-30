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

本文是流计算基础系列的第一篇，讨论并行计算中最基本的问题：一项计算能够被并行到什么程度，以及决定这一上限的因素是什么。

文章从计算之间的依赖关系出发。把一项计算画成 DAG 时，每个节点代表一段可以由线程独立完成的工作，例如读取一批数据、执行一次 join 或合并一组中间结果；每条有向边代表数据依赖，表示下游节点必须等上游产生了对应数据，才具备执行条件。没有依赖边相连的节点可以同时运行，最长的依赖链则决定了增加处理器之后，执行时间还能缩短多少。

现代 DAG 系统通常不要求程序员逐个指定线程的调用次序。程序描述数据要经过哪些变换、按什么键被分区、结果要流向哪些算子，编译器和运行时据此生成执行图，把算子实例部署到 worker，再让每个 worker 从到达自己的数据中选择可执行的工作。换句话说，图的节点编码“收到这类数据时做什么”，边编码“产生的数据送到哪里”。对于不包含循环的计算，这张图既给出了执行次序，也揭示了它与函数式表达式之间的对应关系。

然而，许多计算无法在一张静态的 DAG 中完成。递归查询、图遍历和迭代算法都需要把上一轮的结果送入下一轮，直到满足终止条件。MPI、Pregel 和 Flink 通常以同步轮次组织这类计算；Timely Dataflow 则把逻辑时间附着在数据上，使不同轮次的工作能够同时推进。算子一边处理正在到达的消息，一边通过进度消息持续更新 frontier：数据计算与“未来还可能出现什么时间”的判断始终同时发生。两种方法的差异，实质上是两种表达计算进度的方式。

一次并行计算通常处理一批有限的数据，计算完成，任务也随之结束。如果新的数据持续到来，同一项计算就要反复进行。系统可以用 epoch 标记数据所属的逻辑阶段；前一个 epoch 留下的结果，如果还要参与后一个 epoch 的计算，就形成了状态。状态如何保存、恢复并保持一致，将在后续文章中讨论。

下面先从八个数的求和开始。

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

Timely Dataflow 做了相反的选择：不设全局屏障，让每条消息自己携带逻辑时间戳。这个决定的后果比看上去深刻——它改变了"进度"这个词的含义。我们从时间戳的结构说起。

#### 4.2.1 嵌套时间戳：进入循环，就加一个坐标

先用一个坐标：给每条消息标上它属于第几轮，够吗？对单个循环够了。但真实计算里循环外面还有循环：输入一批接一批到来（批与批之间是 epoch），每一批内部可能要做迭代（iteration），迭代里面还可能再嵌套迭代。一个整数分不清"第 2 批的第 3 轮"和"第 3 批的第 2 轮"。

Timely 的办法是：**时间戳不是一个数，而是一个坐标序列**。进入一层循环作用域，就在末尾追加一个坐标；离开这层作用域，就把它弹掉。第 2 批数据的循环里，第 3 轮的消息时间戳是 `(2, 3)`；如果循环里再套一层循环，内层第 1 轮就是 `(2, 3, 1)`。

这个结构和函数调用栈完全同构：调用一层函数，压一帧；返回，弹一帧。时间戳的长度就是嵌套深度，每个坐标是那一层的局部计数器。

写法上注意，坐标是**扁平的序列**，不是嵌套的二元组：顶层 scope 里时间戳就是 `3`；进入 iterate 后变成 `(3, 0)`；再嵌套一层循环就是 `(3, 0, 0)`。每进入一层作用域，在末尾追加一个坐标；离开时弹出。由此得到一个重要事实：**iterate 之外的算子看不到 iteration 坐标**——无论下游还接着多少算子，它们收到的时间戳只有 `3`，轮次被完整封装在作用域内部。

回边在这个结构里扮演什么角色？**消息每绕回边一圈，最内层坐标加一**——`(2, 3)` 绕一圈变成 `(2, 4)`。这条规则有一个重要推论：消息沿环前进时，时间必然严格增大，不存在"时间在环上原地打转"的消息。frontier 因此在环上单调推进，循环的终止有了数学保证。

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 760 600" role="img" aria-label="逻辑视图：时间戳分层只在 iterate 内部，输入压入坐标、离开弹出，下游不可见。物理视图：三条并行泳道，戊@(3,3) 在 (3,2) 最后一条重复消息到达之前开工，各 worker 的 frontier 独立推进，不存在同步屏障">
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
<text x="380" y="564" text-anchor="middle" class="t-sub">箭头：消息到达即触发下一条计算；frontier（紫色）随完成持续右移——阴影区已封闭、可定稿，三个 worker 互不对齐</text>
<g class="t-sub" fill="#57534e">
<rect x="60" y="580" width="10" height="10" fill="#0f766e"/><text x="76" y="589">新结果立即流动</text>
<rect x="192" y="580" width="10" height="10" fill="#fee2e2" stroke="#b91c1c" stroke-dasharray="2 2"/><text x="208" y="589">重复就地吸收</text>
<line x1="328" y1="580" x2="328" y2="590" stroke="#6d28d9" stroke-width="2" stroke-dasharray="3 2"/><text x="336" y="589">frontier（持续推进）</text>
<line x1="456" y1="580" x2="476" y2="580" stroke="#9a3412" stroke-width="2" stroke-dasharray="5 3"/><text x="484" y="589">BSP 屏障（不存在）</text>
</g>
</svg>
<figcaption class="fig-caption">逻辑视图：时间戳的分层只存在于 iterate scope 内部——输入为 t=3，进入时压入坐标变成 (3,0)，内部按 (3,0)…(3,3) 分阶段（每层标出真实数据），离开时弹出坐标，下游算子只见 t=3。物理视图：同一批消息分布在三个 worker 上按物理时间并行处理——每条消息到达即触发下一条计算（细箭头；红色虚线是产生重复消息的触发），戊@(3,3) 在 (3,2) 的最后一条重复消息到达之前就已开工；frontier（紫色）随每条消息完成而持续右移，阴影区是已封闭、可定稿的时间，三个 worker 的位置互不对齐；橙色虚线是 BSP 会设置的屏障位置，这张图里不存在。逻辑分层是语义，物理不分层、轮次可重叠是执行。</figcaption>
</figure>

#### 4.2.2 偏序：允许"不可比"的时间点

两个时间戳怎么比大小？规则是逐坐标比较，**全部坐标都不落后才算更小**：`(1, 3) ≤ (2, 4)`，因为 1≤2 且 3≤4。那 `(1, 3)` 和 `(2, 1)` 呢？第 1 批的第 3 轮，第 2 批的第 1 轮——第一个坐标落后，第二个坐标超前，**不可比**。

"不可比"不是缺陷，而是整个设计的关键。如果强迫所有时间点排出全局先后（例如改用字典序），系统就必须在批与批之间建立不必要的等待：第 2 批的第 1 轮"排在"第 1 批的第 3 轮后面，就得等它。偏序允许系统说"这两个时间没有先后"——它们的消息并行流动，谁也不等谁。同步轮次里"所有人等齐再走"的约束，被精确地缩小到了真正有依赖的地方。

frontier 就建立在偏序上。它不是一个时间点，而是**一组两两不可比的时间点**，含义是：未来消息的时间戳只会大于等于 frontier 中的某一个，绝不可能更小。把两个坐标画成网格，frontier 是一条向右下推进的阶梯线：

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 720 280" role="img" aria-label="epoch 与 iteration 组成的网格：frontier 是一条阶梯形下界，左下方已封闭，右上方还可能到来，(1,3) 与 (2,1) 不可比">
<g>
<rect x="100" y="95" width="60" height="135" fill="#ede9fe" opacity="0.55"/>
<rect x="100" y="185" width="120" height="45" fill="#ede9fe" opacity="0.55"/>
<rect x="100" y="222" width="240" height="8" fill="#ede9fe" opacity="0.55"/>
</g>
<g fill="#d6d3d1">
<circle cx="100" cy="230" r="2.5"/><circle cx="160" cy="230" r="2.5"/><circle cx="220" cy="230" r="2.5"/><circle cx="280" cy="230" r="2.5"/><circle cx="340" cy="230" r="2.5"/>
<circle cx="100" cy="185" r="2.5"/><circle cx="160" cy="185" r="2.5"/><circle cx="220" cy="185" r="2.5"/><circle cx="280" cy="185" r="2.5"/><circle cx="340" cy="185" r="2.5"/>
<circle cx="100" cy="140" r="2.5"/><circle cx="160" cy="140" r="2.5"/><circle cx="220" cy="140" r="2.5"/><circle cx="280" cy="140" r="2.5"/><circle cx="340" cy="140" r="2.5"/>
<circle cx="100" cy="95" r="2.5"/><circle cx="160" cy="95" r="2.5"/><circle cx="220" cy="95" r="2.5"/><circle cx="280" cy="95" r="2.5"/><circle cx="340" cy="95" r="2.5"/>
<circle cx="100" cy="50" r="2.5"/><circle cx="160" cy="50" r="2.5"/><circle cx="220" cy="50" r="2.5"/><circle cx="280" cy="50" r="2.5"/><circle cx="340" cy="50" r="2.5"/>
</g>
<g stroke="#57534e" stroke-width="1.2" fill="none">
<line x1="92" y1="230" x2="364" y2="230"/><line x1="100" y1="238" x2="100" y2="42"/>
</g>
<text x="372" y="234" class="t-sub">epoch →</text>
<text x="58" y="52" class="t-sub">iteration ↑</text>
<path d="M 100 95 L 160 95 L 160 185 L 220 185 L 220 230 L 340 230" stroke="#6d28d9" stroke-width="2.2" stroke-dasharray="7 5" fill="none"/>
<g fill="#6d28d9">
<circle cx="160" cy="95" r="5"/><circle cx="220" cy="185" r="5"/><circle cx="340" cy="230" r="5"/>
</g>
<g class="t-micro" fill="#6d28d9" font-weight="700">
<text x="172" y="90">(1, 3)</text><text x="232" y="180">(2, 1)</text><text x="318" y="214">(4, 0)</text>
</g>
<g>
<text x="430" y="60" class="t-title">怎么读这张图</text>
<rect x="430" y="78" width="14" height="14" fill="#ede9fe" stroke="#6d28d9" stroke-width="0.8"/><text x="452" y="90" class="t-sub">左下阴影：已封闭，可放心输出、回收状态</text>
<line x1="430" y1="116" x2="444" y2="116" stroke="#6d28d9" stroke-width="2.2" stroke-dasharray="4 3"/><text x="452" y="120" class="t-sub">frontier：一组不可比的时间点（阶梯线）</text>
<text x="452" y="150" class="t-sub">右上方：消息还可能到来</text>
<text x="452" y="180" class="t-sub">(1, 3) 与 (2, 1)：不可比，互不等待</text>
<text x="452" y="210" class="t-sub">算子只需盯住阶梯线，无需全局视图</text>
</g>
</svg>
<figcaption class="fig-caption">frontier 是偏序上的一条阶梯形下界：左下方封闭、右上方待定。两个 frontier 点 (1,3) 与 (2,1) 不可比——它们的消息可以并行流动，这正是轮次重叠的几何含义。</figcaption>
</figure>

阶梯左下方的区域已经"封闭"：时间戳落在那里的消息不可能再来，算子可以放心输出这些轮次的结果、回收这些轮次的状态。注意 frontier 说的是"不会再有"，而不是"已经算完"——算子不需要知道全局发生了什么，只要盯住这条下界线，就能独立做出安全的决定。

这套机制的适用范围也值得记住：Naiad 对时间戳只有一个最低要求——**构成偏序，并且沿回边严格递增**。任何满足这两条的时间体系，都能套用同一套进度追踪协议。

#### 4.2.3 iterate 算子：把回边封装成一次函数调用

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
4. frontier 越过所有已产生的 iteration 时，算子确认不会再有新结果，弹出坐标，把整个循环的结果作为同一 epoch 上的普通输出交给下游。

对外部世界，整个 `iterate` 就是一个普通的映射算子：输入是某个 epoch 上的一批起始公司，输出是同一 epoch 上的穿透结果。**轮次被完整地封装在算子内部**——这是嵌套时间戳的回报：循环不再是图的特殊形状，而是一次可以组合的函数调用。§2 说计算图与函数表达式同构，到这里，递归函数也被收编了进来。

#### 4.2.4 同一条查询，按逻辑时间推演

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

**追问一：iterate 会攒够一批消息再处理吗？** 不会。数据面上消息逐条立即处理——物理时间轴里 `戊@(3,3)` 抢在 `(3,2)` 的最后一条消息之前流动，就是证据。唯一会"等"的是 frontier 这条控制通道，而它只回答"哪个时间戳可以定稿、状态可以回收"，不拦截数据。

**追问二：那重复消息的代价怎么压低？** 靠合并（consolidation）。Differential 的每条更新是（数据， 时间， 权重）三元组，系统会随手把同一（数据， 时间）的更新合并：权重相加，抵消为零的直接删除。效果立竿见影——同一轮里先 +1 又 −1，合并后等于什么都没发生，下游零工作量；同一个 key 在同一时间被两次 +1，合并成 +2，distinct 只需输出一次。注意这是"随到随合并"，不是"攒够了再发"：合并不引入任何等待，只是把废更新消灭在传播途中。worker 之间的网络打包同理，是吞吐优化，对语义透明。

到第二篇，（data, time, diff) 三元组会成为主角，合并将作为一等机制再出现。

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
| 完成判定 | 子树关闭、工作表为空、driver 结束 | frontier 越过相关时间点，状态和输出可定稿 |

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

- Naiad: A Timely Dataflow System (SOSP'13) §2–3：嵌套时间戳、progress tracking、frontier
- Differential Dataflow (CIDR'13)：`iterate` 与 semi-naïve 求值
- Malewicz et al., "Pregel" (SIGMOD 2010)：superstep 同步迭代
- Valiant, "A Bridging Model for Parallel Computation" (1990)：BSP 模型
- Blelloch, "Prefix Sums and Their Applications"：扫描算法的深度与工作量权衡
- Aho & Ullman, "Universality of Data Retrieval Languages" (POPL 1979)：关系代数表达不了传递闭包
- Gray et al., "Data Cube" (1997)：聚合算子的 distributive / algebraic / holistic 分类
- Finkelstein et al., "Expressing Recursive Queries in SQL" (X3H2-96-075)：递归查询进入 SQL 标准的来龙去脉
