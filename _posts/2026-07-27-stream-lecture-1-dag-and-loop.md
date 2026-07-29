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
.post-content table { display: block; overflow-x: auto; border-collapse: collapse; width: 100%; font-size: 0.9375rem; line-height: 1.6; }
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
</style>

本文是流计算基础系列的第一篇，讨论并行计算中最基本的问题：一项计算能够被并行到什么程度，以及决定这一上限的因素是什么。

文章从计算之间的依赖关系出发。依赖关系决定了任务必须按照怎样的次序执行，也决定了增加处理器之后，执行时间还能缩短多少。当这些依赖被表示成图，便得到并行计算中常见的有向无环图，也就是 DAG。对于不包含循环的计算，这张图既描述了程序的执行次序，也揭示了它与函数式表达式之间的对应关系。

然而，许多计算无法在一张静态的 DAG 中完成。递归查询、图遍历和迭代算法都需要把上一轮的结果送入下一轮，直到满足终止条件。MPI、Pregel 和 Flink 通常以同步轮次组织这类计算；Timely Dataflow 则把逻辑时间附着在数据上，使不同轮次的工作能够同时推进。两种方法的差异，实质上是两种表达计算进度的方式。

一次并行计算通常处理一批有限的数据，计算完成，任务也随之结束。如果新的数据持续到来，同一项计算就要反复进行。系统可以用 epoch 标记数据所属的逻辑阶段；前一个 epoch 留下的结果，如果还要参与后一个 epoch 的计算，就形成了状态。状态如何保存、恢复并保持一致，将在后续文章中讨论。

下面先从八个数的求和开始。

## 1. 一个例子：并行计算的极限是依赖图的长度

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

- **工作量（work）**：全部加法的次数，7 次。它决定你要付多少电费。
- **关键路径（critical path，也叫 span）**：图里最长的那条依赖链。它决定你最少要等多久——哪怕有无限多台机器，也快不过这条链。理论说法是 Brent 定理：p 台机器的执行时间 `T(p) ≤ T₁/p + T∞`，机器趋于无限时，剩下的只有 T∞，也就是关键路径的长度。

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

更直接的办法是加入一条回边，把本轮的新结果重新送回连接操作。此时，算子结构本身可以保持不变：同一个 join 被重复使用，不必为每一轮复制一套算子。但是图中出现回边以后，仅靠原来的依赖关系已经无法决定执行顺序。系统还必须知道一条记录属于哪一轮，以及未来是否还会有新的记录进入这一轮。

因此，循环需要补充两项机制：

1. **轮次或逻辑时间**：区分同一条回边上不同迭代产生的数据；
2. **进度判断**：确定某一轮是否已经结束，以及整个循环是否已经收敛。

这两个问题构成了后文比较同步迭代与 Timely Dataflow 的基础。

### 3.2 去重不仅影响性能，也决定能否终止

前面的例子中，丙公司会被发现三次：P 直接持有丙，甲持有丙，乙也持有丙。如果每次发现都不加区分地重新送入下一轮，即使没有环，也会产生重复计算。若持股关系中存在交叉持股，例如甲持有乙、乙又持有甲，问题会更加明显：记录会沿着环不断返回，计算永远不会停止。

因此，每一轮都要区分“已经见过的公司”和“本轮第一次发现的公司”。只有后者才需要进入下一轮。前面公式中的 `distinct` 和集合差并非单纯的性能优化，它们同时给出了集合型递归的终止条件：当一轮计算不再产生新公司时，已知集合达到不动点，循环结束。

这一点也说明了运行时展开的实质。系统并不是随意创建一张结构不断变化的图，而是在重复执行同一段计算结构；真正动态变化的是每轮输入的数据，以及由这些数据决定的迭代次数。
