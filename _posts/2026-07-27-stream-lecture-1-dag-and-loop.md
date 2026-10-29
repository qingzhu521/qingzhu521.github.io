---
layout: post
title: "流计算基础（一）：从并行计算到数据流 —— 依赖、DAG 与执行"
date: 2026-07-27 10:00:00 +0800
categories: stream-processing
tags: [并行计算, dag, 执行模型]
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

> 系列阅读：[一：并行与 DAG]({{ '/2026/07/27/stream-lecture-1-dag-and-loop/' | relative_url }}) · [二：循环与无界计算]({{ '/2026/10/28/stream-lecture-2-loops-and-unbounded-computation/' | relative_url }}) · [三：事件时间、状态与容错]({{ '/2026/08/05/stream-lecture-2-time-state-and-fault-tolerance/' | relative_url }})

本文讨论一个问题：**一项计算怎样从依赖关系变成并行执行？**

从求和与前缀和出发，先看工作量和关键路径，再用 DAG 表达依赖，最后回到数据库执行引擎，区分算子图、pipeline、线程和调度。这样，看到一张计算图时，就能说明哪些工作可以同时做，哪些必须等待。

本篇以一次有限输入的计算为主。循环怎样表达、持续输入中怎样确认局部结果完整，由第二篇沿同一套概念继续展开。

## 1. 关键路径：依赖关系给并行计算划定的极限

许多并行算法都遵循分而治之的思路：先把原问题分解成若干可以独立处理的子问题，再把各个子问题的结果合并起来。分解决定哪些工作可以同时进行，合并则建立了子问题之间的先后关系。这些关系共同构成了计算的依赖结构。

先用整数加法，避免浮点重排的舍入差异。拿 8 个数 `[3, 1, 4, 1, 5, 9, 2, 6]` 算两笔账。

第一笔，求和。哪两个数先加都不影响结果，8 个数可以组织成一棵 3 层的树：4 对同时加，2 个中间和同时加，最后一次收尾。7 次加法，3 层算完，终点 31。

第二笔，前缀和：第 i 个位置要输出前 i 个数之和。按最直观的串行算法，用第 4 个位置的结果计算第 5 个位置，每一步都等待上一步。这个算法把 7 次加法连成一条链，最后一个输出同样是 31；整组输出则是 `[3, 4, 8, 9, 14, 23, 25, 31]`。依赖链属于这套算法，后面会看到怎样把前缀和也改成并行计算。

<figure class="fig-card">
<svg class="fig-svg" viewBox="0 0 720 350" role="img" aria-label="求和组成深度3的树，前缀和连成深度7的链">
<defs>
<marker id="fa-teal" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
<marker id="fa-orange" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#9a3412"/></marker>
</defs>
<text x="170" y="28" text-anchor="middle" class="t-title" fill="#0f766e">求和 · 同层并行</text>
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
<figcaption class="fig-caption">同样 8 个数、同样 7 次加法。左边同层计算互不等待，3 层算完；右边每一步都在等上一步，7 层一层不少。机器再多，也快不过图里最长那条链。</figcaption>
</figure>

同样的数据，同样的计算量，快慢差了两倍多——差距不在机器，在图的形状。这里有两个名词值得记住：

- **工作量（work）**：全部加法的次数，7 次。它等于只用一台机器串行跑完所需的时间（T₁）——无论投入多少机器，这笔总账不会变少，只是被分摊到同一时刻并行支付。
- **关键路径（critical path，也叫 span）**：图里最长的那条依赖链。它等于有无限多机器时的最短完成时间（T∞）——机器加得再多，也快不过这条链。在单位成本、忽略通信且采用适当调度的理想模型中，Brent 定理给出上界：p 台机器的执行时间 `T(p) ≤ T₁/p + T∞`，机器趋于无限时，剩下的只有 T∞，也就是关键路径的长度。

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

这两个例子让依赖结构变得具体：同样的计算目标，可以由不同算法组织成不同形状。接下来把这种画法推广到一般的计算图，再看执行引擎怎样调度它。

<div class="callout callout--insight">
<p><strong>归因</strong>：这一节回答的问题是"为什么我加了机器，程序还是不快"。排查顺序是固定的：先找到关键路径，看自己卡在链上还是卡在宽度上。卡在链上，加机器没用——要么换算法把链压短，要么像 Hillis-Steele 这样拿工作量换深度。</p>
</div>

## 2. 从依赖关系到静态计算图

前面的两种算法都可以画成图。图中的节点表示一次计算，箭头表示计算之间的数据依赖。如果节点 B 需要读取节点 A 的结果，就从 A 向 B 画一条箭头。箭头同时规定了执行顺序：B 所需的数据尚未由 A 产生时，对应的计算就不能开始。

只要图中不存在循环，这些依赖就构成一张有向无环图，即 DAG。所谓“无环”，是指沿着箭头一直前进，不会重新回到已经经过的节点。若图的规模有限、各节点本身都能结束，执行就可以沿依赖关系推进到最终结果。无环结构本身并不保证任意算子内部都能终止。

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

### 2.2 计算图的层次

求和图把每次加法画成一个节点；SQL 执行计划把处理许多记录的逻辑封装成一个算子。两者都能用 DAG 描述，但节点的粒度不同，不能直接把一个节点等同于一个线程或一次函数调用。

静态的算子图也不意味着输入规模固定：同一套 scan、filter、join 可以处理不同数量的记录。图规定算子之间的依赖和数据路径，记录数量决定这些算子实际完成多少工作。

对于需要反复应用同一规则的任务，引擎还可以把循环放在算子内部或外部 driver 中，使外层计划保持 DAG。第二篇会把这一层打开，研究轮次之间的依赖。本篇先看一次执行中，算子、pipeline 和线程怎样配合。


## 3. 执行引擎怎样把图跑起来

### 3.1 火山模型：一次查询的一生

经典数据库执行引擎常以迭代器模型（通常称为 Volcano model）组织执行：执行计划是一棵算子树，每个算子对外只暴露三个接口——`open()`、`next()`、`close()`。根节点要一条数据，就调用子算子的 `next()`，子算子再向自己的子算子要，一路向下；数据沿调用栈一路向上返回。控制流向下，数据流向上，一次 `next()` 搬运一条元组。Graefe 的 Volcano 系统系统化地讨论了这种接口及其并行扩展。

拉模型（pull）当年胜出，理由都很实际：

1. 流控由调用关系自然带出——在同一条同步调用链内，消费者不继续调用 `next()`，上游就不继续为它生产；跨进程缓冲仍需单独流控；
2. 组合性——每个算子是黑盒，优化器可以自由替换实现；
3. 阻塞算子有现成的表达方式——sort、hash join 的 build 侧只需要在第一次被调用时把输入吃光；
4. 可以按需停止取数——取消正在运行的计算和回收资源，仍需相应的取消与清理机制；
5. 同一进程内可由一个线程沿调用栈驱动多个算子，避免为每对相邻算子设置通信队列。

注意第 3 条里的分界。执行引擎的经典视角是把计划树沿阻塞算子（pipeline breaker）切开：scan、filter、project 这类算子随产随消，它们组成的极大链就是一条 pipeline，内部可以逐条传递，通常不必把整份中间关系物化；碰到全量 sort、阻塞式聚合、常规 hash join 的 build 侧，数据必须物化，成为下一条 pipeline 的输入。**pipeline breaker 划出数据必须先准备好的边界；各段的执行时机和线程分配则由运行时决定。**

<figure class="fig-card fig-card--dense">
<svg class="fig-svg" viewBox="0 0 800 440" role="img" aria-label="左：算子树，HashBuild 与 Sort 标为 breaker，切口画在 breaker 处；右：三段 pipeline 通过哈希表与有序数据交接，编号表示执行阶段而非固定线程">
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
<text x="417" y="82" class="t-micro" fill="#9a3412" font-weight="700">P1 · 阶段 1</text>
<rect x="417" y="90" width="82" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="458" y="107" text-anchor="middle" class="t-label">Scan·cust</text>
<line x1="499" y1="103" x2="509" y2="103" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="511" y="90" width="96" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="559" y="107" text-anchor="middle" class="t-label">构建哈希表</text>
<line x1="607" y1="103" x2="617" y2="103" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="619" y="90" width="124" height="26" rx="7" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/><text x="681" y="107" text-anchor="middle" class="t-label">哈希表 · 物化</text>
<rect x="405" y="184" width="375" height="64" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="417" y="202" class="t-micro" fill="#9a3412" font-weight="700">P2 · 阶段 2</text>
<rect x="417" y="210" width="88" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="461" y="227" text-anchor="middle" class="t-label">Scan·orders</text>
<line x1="505" y1="223" x2="515" y2="223" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="517" y="210" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="549" y="227" text-anchor="middle" class="t-label">Filter</text>
<line x1="581" y1="223" x2="591" y2="223" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="593" y="210" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="625" y="227" text-anchor="middle" class="t-label">⋈ probe</text>
<line x1="657" y1="223" x2="667" y2="223" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="669" y="210" width="98" height="26" rx="7" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/><text x="718" y="227" text-anchor="middle" class="t-label">有序数据·物化</text>
<rect x="405" y="304" width="375" height="64" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="417" y="322" class="t-micro" fill="#9a3412" font-weight="700">P3 · 阶段 3</text>
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
<text x="592" y="410" text-anchor="middle" class="t-sub">三段 pipeline 属于同一次查询；线程与计划对象可以由运行时复用</text>
</svg>
<figcaption class="fig-caption">左：一棵执行计划树，橙色节点是 pipeline breaker——hash join 的 build 侧与 Sort，数据必须在这里物化才能继续向上。右：沿两个 breaker 切开，得到三条 pipeline，物化点是流水线之间的交接。这是阶段划分，不要求一段绑定一个独占线程，也不要求三段同时开始、同时结束。</figcaption>
</figure>

并行化也在同一框架里完成：Graefe 把 shuffle 包装成一个普通算子（Exchange）插进树里，树内走流水线，跨 exchange 换线程、换机器。这使数据处理逻辑与跨进程分区、传输能够分别实现。现代引擎还会使用向量化、编译执行和任务调度等机制，具体 pipeline 与线程的对应需要结合运行时来看。

### 3.2 DAG、Pipeline、Dataflow 分别描述什么

数据库和流系统都会用到这三个词。把它们放到同一条执行路径上，分工就清楚了。

**DAG 描述依赖结构。** 节点表示所选粒度上的计算，边表示谁需要谁的数据。它限制合法的执行顺序，但不会替我们决定每个节点分配多少线程、放在哪台机器。

**Pipeline 描述连续处理的执行片段。** scan、filter、project 可以把数据逐步传下去；阻塞式算子要求先准备足够的输入，再进入下一阶段。跨线程或网络的 exchange 划出通信边界，但通信边界不一定要求把全量数据物化，流水传输也可以跨越它。

**Dataflow 描述计算与数据路径的组织。** 运行时把算子连接起来，沿边传递数据并满足依赖。它可以采用需求驱动，也可以采用数据到达后激活算子的调度方式；Volcano 的 exchange 就连接了进程内的需求驱动与进程间的数据驱动。带反馈的 dataflow 图也可以表达循环，这部分放到第二篇。

| 观察层次 | 要回答的问题 | 典型内容 |
|---|---|---|
| 依赖结构 | 哪些工作必须先后发生 | DAG、算子树、数据依赖边 |
| 执行片段 | 哪些算子可以连续处理，在哪里等待或交接 | pipeline、物化点、exchange |
| 执行驱动 | 谁发起下一步处理 | `next()` 调用、消息就绪、运行时调度 |

以 Flink 的 operator chaining 为例，多个算子实例可以串在同一个 task 的线程中，跨 task 再通过通道传递数据。它是具体系统对执行片段和调度边界的选择，不能反过来把所有 pipeline 都定义成一条线程。参见 [Flink 的任务与算子链](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/concepts/flink-architecture/#tasks-and-operator-chains)。

到这里，可以沿三个层次读一张执行计划：先看依赖限制，再看数据在哪里等待或交接，最后看运行时如何分配和驱动工作。增加机器主要改变最后一层；改写算法还可能改变第一层的关键路径。


## 4. 下一步：当计算需要重复执行

前缀和按跨度 1、2、4 依次合并时，已经出现了重复应用同一条规则的过程。若长度由运行时输入决定，程序可以把这个过程写成循环；更一般的可达性查询，还要依据当前发现的结果决定是否继续。

DAG 给出某一层次上的依赖结构，运行时据此安排并行。把循环本身打开后，还需要说明下一轮接收什么，以及如何判断后续工作已经结束。输入持续变化时，这个判断又必须落到一个个可关闭的逻辑范围上。

[第二篇：循环与无界计算]({{ '/2026/10/28/stream-lecture-2-loops-and-unbounded-computation/' | relative_url }})从持股查询开始，逐步讨论同步轮次、逻辑时间、capability、frontier，再追踪持续新增与删除怎样通过循环改变结果。

## 延伸阅读

- R. P. Brent, "The Parallel Evaluation of General Arithmetic Expressions" (JACM 1974)：工作量、关键路径与处理器数量的关系。
- Blelloch, "Prefix Sums and Their Applications"：扫描算法的深度与工作量权衡。
- [Goetz Graefe, Volcano—An Extensible and Parallel Query Evaluation System](https://awoc.wolski.fi/dlib/parallel-query/Graefe94-Volcano.pdf) (IEEE TKDE 1994)：迭代器接口与 exchange 的并行扩展。
- [配套 slides：从数据库看流计算]({{ '/slides/flink-for-db-developers.html' | relative_url }})：系列补充材料。
