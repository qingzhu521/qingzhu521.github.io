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

**图的长度决定并行时间下限。**

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

### 3.1 从计算图到数据流

第二章的节点可以表示一次加法，也可以表示一个算子。接下来，我们采用算子这一层次。每个算子按照规定的规则处理输入。它把结果交给后续算子。

我们用**数据流（dataflow）**描述这种组织方式。节点表示算子。边连接一个算子的输出与另一个算子的输入。下面用两条查询举例。

假设订单表包含 512 个分区。集群中有 64 台机器。每台机器可以同时运行 8 个任务。引擎把每个分区交给一个任务。

**例一：非聚合查询。** 每个任务扫描自己的分区。过滤算子保留金额不低于 100 的订单。投影算子输出订单号。各任务独立写出结果。

```sql
SELECT order_id
FROM orders
WHERE amount >= 100;
```

<figure id="dataflow-row-example" class="fig-card fig-card--dense">
<svg class="fig-svg" viewBox="0 0 840 636" role="img" aria-label="64台机器各运行8个任务，共512个并行任务。每个任务扫描、过滤并投影自己的输入分区。各任务独立写出结果分区。图中展示机器1、机器2和机器64，其余机器用省略号表示。">
<defs>
<marker id="dataflow-row-example-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#78716c"/></marker>
<marker id="dataflow-row-example-merge" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#0f766e"/></marker>
</defs>
<text x="16" y="26" text-anchor="start" font-size="18" font-weight="600" fill="#1c1917">非聚合：512 个任务独立处理各自的分区</text>
<text x="16" y="51" text-anchor="start" font-size="14" font-weight="400" fill="#57534e">示例配置：64 台机器 × 每台 8 个任务 = 512 路并行</text>
<text x="16" y="76" text-anchor="start" font-size="12" font-weight="400" fill="#57534e">算子定义</text>
<rect x="16" y="88" width="244" height="56" rx="8" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="138" y="111" text-anchor="middle" font-size="15" font-weight="600" fill="#1c1917">扫描算子</text>
<text x="138" y="132" text-anchor="middle" font-size="13" font-weight="400" fill="#57534e">读取 orders</text>
<rect x="296" y="88" width="244" height="56" rx="8" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="418" y="111" text-anchor="middle" font-size="15" font-weight="600" fill="#1c1917">过滤算子</text>
<text x="418" y="132" text-anchor="middle" font-size="13" font-weight="400" fill="#57534e">amount ≥ 100</text>
<rect x="580" y="88" width="244" height="56" rx="8" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="702" y="111" text-anchor="middle" font-size="15" font-weight="600" fill="#1c1917">投影算子</text>
<text x="702" y="132" text-anchor="middle" font-size="13" font-weight="400" fill="#57534e">保留 order_id</text>
<line x1="264" y1="116" x2="291" y2="116" stroke="#78716c" stroke-width="1.6" marker-end="url(#dataflow-row-example-arrow)"/>
<line x1="544" y1="116" x2="575" y2="116" stroke="#78716c" stroke-width="1.6" marker-end="url(#dataflow-row-example-arrow)"/>
<line x1="16" y1="160" x2="824" y2="160" stroke="#e8e0d4" stroke-dasharray="4 4"/>
<text x="420" y="184" text-anchor="middle" font-size="14" font-weight="600" fill="#1c1917">展开执行实例：每个分区执行一份扫描、过滤和后续计算</text>
<rect x="16" y="202" width="244" height="350" rx="12" fill="#f6f1e7" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="32" y="226" text-anchor="start" font-size="16" font-weight="600" fill="#1c1917">机器 1</text>
<text x="244" y="226" text-anchor="end" font-size="12" font-weight="400" fill="#57534e">8 个并行任务</text>
<rect x="28" y="248" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="38" y="266" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 1 · 输入分区 1</text>
<rect x="36" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="65" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="95" y1="286" x2="106" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="108" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="137" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="167" y1="286" x2="178" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="180" y="274" width="58" height="24" rx="5" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="209" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">投影</text>
<text x="138" y="314" text-anchor="middle" font-size="12" font-weight="400" fill="#0f766e">独立写出结果分区 1</text>
<rect x="28" y="330" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="38" y="348" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 2 · 输入分区 2</text>
<rect x="36" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="65" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="95" y1="368" x2="106" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="108" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="137" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="167" y1="368" x2="178" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="180" y="356" width="58" height="24" rx="5" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="209" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">投影</text>
<text x="138" y="396" text-anchor="middle" font-size="12" font-weight="400" fill="#0f766e">独立写出结果分区 2</text>
<rect x="28" y="440" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="38" y="458" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 8 · 输入分区 8</text>
<rect x="36" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="65" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="95" y1="478" x2="106" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="108" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="137" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="167" y1="478" x2="178" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="180" y="466" width="58" height="24" rx="5" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="209" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">投影</text>
<text x="138" y="506" text-anchor="middle" font-size="12" font-weight="400" fill="#0f766e">独立写出结果分区 8</text>
<text x="138" y="428" text-anchor="middle" font-size="25" font-weight="400" fill="#78716c">⋮</text>
<text x="138" y="538" text-anchor="middle" font-size="12" font-weight="400" fill="#57534e">任务 1—8 各处理一个分区</text>
<rect x="296" y="202" width="244" height="350" rx="12" fill="#f6f1e7" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="312" y="226" text-anchor="start" font-size="16" font-weight="600" fill="#1c1917">机器 2</text>
<text x="524" y="226" text-anchor="end" font-size="12" font-weight="400" fill="#57534e">8 个并行任务</text>
<rect x="308" y="248" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="318" y="266" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 9 · 输入分区 9</text>
<rect x="316" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="345" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="375" y1="286" x2="386" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="388" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="417" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="447" y1="286" x2="458" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="460" y="274" width="58" height="24" rx="5" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="489" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">投影</text>
<text x="418" y="314" text-anchor="middle" font-size="12" font-weight="400" fill="#0f766e">独立写出结果分区 9</text>
<rect x="308" y="330" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="318" y="348" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 10 · 输入分区 10</text>
<rect x="316" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="345" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="375" y1="368" x2="386" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="388" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="417" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="447" y1="368" x2="458" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="460" y="356" width="58" height="24" rx="5" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="489" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">投影</text>
<text x="418" y="396" text-anchor="middle" font-size="12" font-weight="400" fill="#0f766e">独立写出结果分区 10</text>
<rect x="308" y="440" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="318" y="458" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 16 · 输入分区 16</text>
<rect x="316" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="345" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="375" y1="478" x2="386" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="388" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="417" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="447" y1="478" x2="458" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="460" y="466" width="58" height="24" rx="5" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="489" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">投影</text>
<text x="418" y="506" text-anchor="middle" font-size="12" font-weight="400" fill="#0f766e">独立写出结果分区 16</text>
<text x="418" y="428" text-anchor="middle" font-size="25" font-weight="400" fill="#78716c">⋮</text>
<text x="418" y="538" text-anchor="middle" font-size="12" font-weight="400" fill="#57534e">任务 9—16 各处理一个分区</text>
<rect x="580" y="202" width="244" height="350" rx="12" fill="#f6f1e7" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="596" y="226" text-anchor="start" font-size="16" font-weight="600" fill="#1c1917">机器 64</text>
<text x="808" y="226" text-anchor="end" font-size="12" font-weight="400" fill="#57534e">8 个并行任务</text>
<rect x="592" y="248" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="602" y="266" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 505 · 输入分区 505</text>
<rect x="600" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="629" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="659" y1="286" x2="670" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="672" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="701" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="731" y1="286" x2="742" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="744" y="274" width="58" height="24" rx="5" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="773" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">投影</text>
<text x="702" y="314" text-anchor="middle" font-size="12" font-weight="400" fill="#0f766e">独立写出结果分区 505</text>
<rect x="592" y="330" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="602" y="348" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 506 · 输入分区 506</text>
<rect x="600" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="629" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="659" y1="368" x2="670" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="672" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="701" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="731" y1="368" x2="742" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="744" y="356" width="58" height="24" rx="5" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="773" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">投影</text>
<text x="702" y="396" text-anchor="middle" font-size="12" font-weight="400" fill="#0f766e">独立写出结果分区 506</text>
<rect x="592" y="440" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="602" y="458" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 512 · 输入分区 512</text>
<rect x="600" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="629" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="659" y1="478" x2="670" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="672" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="701" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="731" y1="478" x2="742" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-row-example-arrow)"/>
<rect x="744" y="466" width="58" height="24" rx="5" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="773" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">投影</text>
<text x="702" y="506" text-anchor="middle" font-size="12" font-weight="400" fill="#0f766e">独立写出结果分区 512</text>
<text x="702" y="428" text-anchor="middle" font-size="25" font-weight="400" fill="#78716c">⋮</text>
<text x="702" y="538" text-anchor="middle" font-size="12" font-weight="400" fill="#57534e">任务 505—512 各处理一个分区</text>
<text x="560" y="371" text-anchor="middle" font-size="23" font-weight="400" fill="#78716c">⋯</text>
<rect x="16" y="576" width="808" height="44" rx="8" fill="#e7f4ef" stroke="#0f766e" stroke-width="1.3"/>
<text x="420" y="603" text-anchor="middle" font-size="15" font-weight="600" fill="#0f766e">各任务独立写出结果。输出仍保留为 512 个分区。</text>
</svg>
<figcaption class="fig-caption">每行表示一个任务。每个任务都有自己的算子实例。各任务可以并行执行。省略号代表未画出的机器和任务。</figcaption>
</figure>

**例二：聚合查询。** 每个任务先计算本分区的局部和。各机器再合并本机结果。后续任务继续分层合并。本例采用整数求和。

```sql
SELECT SUM(amount)
FROM orders
WHERE amount >= 100;
```

<figure id="dataflow-sum-example" class="fig-card fig-card--dense">
<svg class="fig-svg" viewBox="0 0 840 862" role="img" aria-label="64台机器各运行8个任务，共512个并行任务。每个任务扫描并过滤一个分区，再计算局部和。各机器合并本机8个局部和。8个合并任务各合并8台机器的结果。最终任务合并这8个结果。">
<defs>
<marker id="dataflow-sum-example-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#78716c"/></marker>
<marker id="dataflow-sum-example-merge" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#9a3412"/></marker>
</defs>
<text x="16" y="26" text-anchor="start" font-size="18" font-weight="600" fill="#1c1917">聚合：512 个任务求局部和，再分层合并</text>
<text x="16" y="51" text-anchor="start" font-size="14" font-weight="400" fill="#57534e">示例配置：64 台机器 × 每台 8 个任务 = 512 路并行</text>
<text x="16" y="76" text-anchor="start" font-size="12" font-weight="400" fill="#57534e">算子定义</text>
<rect x="16" y="88" width="244" height="56" rx="8" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="138" y="111" text-anchor="middle" font-size="15" font-weight="600" fill="#1c1917">扫描算子</text>
<text x="138" y="132" text-anchor="middle" font-size="13" font-weight="400" fill="#57534e">读取 orders</text>
<rect x="296" y="88" width="244" height="56" rx="8" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="418" y="111" text-anchor="middle" font-size="15" font-weight="600" fill="#1c1917">过滤算子</text>
<text x="418" y="132" text-anchor="middle" font-size="13" font-weight="400" fill="#57534e">amount ≥ 100</text>
<rect x="580" y="88" width="244" height="56" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="702" y="111" text-anchor="middle" font-size="15" font-weight="600" fill="#1c1917">聚合算子</text>
<text x="702" y="132" text-anchor="middle" font-size="13" font-weight="400" fill="#57534e">SUM(amount)</text>
<line x1="264" y1="116" x2="291" y2="116" stroke="#78716c" stroke-width="1.6" marker-end="url(#dataflow-sum-example-arrow)"/>
<line x1="544" y1="116" x2="575" y2="116" stroke="#78716c" stroke-width="1.6" marker-end="url(#dataflow-sum-example-arrow)"/>
<line x1="16" y1="160" x2="824" y2="160" stroke="#e8e0d4" stroke-dasharray="4 4"/>
<text x="420" y="184" text-anchor="middle" font-size="14" font-weight="600" fill="#1c1917">展开执行实例：每个分区执行一份扫描、过滤和后续计算</text>
<rect x="16" y="202" width="244" height="350" rx="12" fill="#f6f1e7" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="32" y="226" text-anchor="start" font-size="16" font-weight="600" fill="#1c1917">机器 1</text>
<text x="244" y="226" text-anchor="end" font-size="12" font-weight="400" fill="#57534e">8 个并行任务</text>
<rect x="28" y="248" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="38" y="266" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 1 · 输入分区 1</text>
<rect x="36" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="65" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="95" y1="286" x2="106" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="108" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="137" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="167" y1="286" x2="178" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="180" y="274" width="58" height="24" rx="5" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="209" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">局部和</text>
<text x="138" y="314" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">局部结果 s1</text>
<path d="M238 286 H253" fill="none" stroke="#9a3412" stroke-width="1.4"/>
<rect x="28" y="330" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="38" y="348" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 2 · 输入分区 2</text>
<rect x="36" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="65" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="95" y1="368" x2="106" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="108" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="137" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="167" y1="368" x2="178" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="180" y="356" width="58" height="24" rx="5" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="209" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">局部和</text>
<text x="138" y="396" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">局部结果 s2</text>
<path d="M238 368 H253" fill="none" stroke="#9a3412" stroke-width="1.4"/>
<rect x="28" y="440" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="38" y="458" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 8 · 输入分区 8</text>
<rect x="36" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="65" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="95" y1="478" x2="106" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="108" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="137" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="167" y1="478" x2="178" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="180" y="466" width="58" height="24" rx="5" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="209" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">局部和</text>
<text x="138" y="506" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">局部结果 s8</text>
<path d="M238 478 H253" fill="none" stroke="#9a3412" stroke-width="1.4"/>
<text x="138" y="428" text-anchor="middle" font-size="25" font-weight="400" fill="#78716c">⋮</text>
<text x="138" y="538" text-anchor="middle" font-size="12" font-weight="400" fill="#57534e">任务 1—8 各处理一个分区</text>
<path d="M253 286 V562 H138 V574" fill="none" stroke="#9a3412" stroke-width="1.5" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="51" y="574" width="174" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="138" y="593" text-anchor="middle" font-size="14" font-weight="600" fill="#9a3412">机器内合并</text>
<text x="138" y="611" text-anchor="middle" font-size="11" font-weight="400" fill="#9a3412">8 个局部结果 → 1 个结果</text>
<rect x="296" y="202" width="244" height="350" rx="12" fill="#f6f1e7" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="312" y="226" text-anchor="start" font-size="16" font-weight="600" fill="#1c1917">机器 2</text>
<text x="524" y="226" text-anchor="end" font-size="12" font-weight="400" fill="#57534e">8 个并行任务</text>
<rect x="308" y="248" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="318" y="266" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 9 · 输入分区 9</text>
<rect x="316" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="345" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="375" y1="286" x2="386" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="388" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="417" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="447" y1="286" x2="458" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="460" y="274" width="58" height="24" rx="5" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="489" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">局部和</text>
<text x="418" y="314" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">局部结果 s9</text>
<path d="M518 286 H533" fill="none" stroke="#9a3412" stroke-width="1.4"/>
<rect x="308" y="330" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="318" y="348" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 10 · 输入分区 10</text>
<rect x="316" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="345" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="375" y1="368" x2="386" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="388" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="417" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="447" y1="368" x2="458" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="460" y="356" width="58" height="24" rx="5" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="489" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">局部和</text>
<text x="418" y="396" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">局部结果 s10</text>
<path d="M518 368 H533" fill="none" stroke="#9a3412" stroke-width="1.4"/>
<rect x="308" y="440" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="318" y="458" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 16 · 输入分区 16</text>
<rect x="316" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="345" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="375" y1="478" x2="386" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="388" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="417" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="447" y1="478" x2="458" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="460" y="466" width="58" height="24" rx="5" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="489" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">局部和</text>
<text x="418" y="506" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">局部结果 s16</text>
<path d="M518 478 H533" fill="none" stroke="#9a3412" stroke-width="1.4"/>
<text x="418" y="428" text-anchor="middle" font-size="25" font-weight="400" fill="#78716c">⋮</text>
<text x="418" y="538" text-anchor="middle" font-size="12" font-weight="400" fill="#57534e">任务 9—16 各处理一个分区</text>
<path d="M533 286 V562 H418 V574" fill="none" stroke="#9a3412" stroke-width="1.5" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="331" y="574" width="174" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="418" y="593" text-anchor="middle" font-size="14" font-weight="600" fill="#9a3412">机器内合并</text>
<text x="418" y="611" text-anchor="middle" font-size="11" font-weight="400" fill="#9a3412">8 个局部结果 → 1 个结果</text>
<rect x="580" y="202" width="244" height="350" rx="12" fill="#f6f1e7" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="596" y="226" text-anchor="start" font-size="16" font-weight="600" fill="#1c1917">机器 64</text>
<text x="808" y="226" text-anchor="end" font-size="12" font-weight="400" fill="#57534e">8 个并行任务</text>
<rect x="592" y="248" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="602" y="266" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 505 · 输入分区 505</text>
<rect x="600" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="629" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="659" y1="286" x2="670" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="672" y="274" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="701" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="731" y1="286" x2="742" y2="286" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="744" y="274" width="58" height="24" rx="5" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="773" y="291" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">局部和</text>
<text x="702" y="314" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">局部结果 s505</text>
<path d="M802 286 H817" fill="none" stroke="#9a3412" stroke-width="1.4"/>
<rect x="592" y="330" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="602" y="348" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 506 · 输入分区 506</text>
<rect x="600" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="629" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="659" y1="368" x2="670" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="672" y="356" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="701" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="731" y1="368" x2="742" y2="368" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="744" y="356" width="58" height="24" rx="5" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="773" y="373" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">局部和</text>
<text x="702" y="396" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">局部结果 s506</text>
<path d="M802 368 H817" fill="none" stroke="#9a3412" stroke-width="1.4"/>
<rect x="592" y="440" width="220" height="74" rx="8" fill="#fff" stroke="#e8e0d4" stroke-width="1.3"/>
<text x="602" y="458" text-anchor="start" font-size="12" font-weight="600" fill="#57534e">任务 512 · 输入分区 512</text>
<rect x="600" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="629" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">扫描</text>
<line x1="659" y1="478" x2="670" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="672" y="466" width="58" height="24" rx="5" fill="#fff" stroke="#d6d3d1" stroke-width="1.3"/>
<text x="701" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">过滤</text>
<line x1="731" y1="478" x2="742" y2="478" stroke="#78716c" stroke-width="1.2" marker-end="url(#dataflow-sum-example-arrow)"/>
<rect x="744" y="466" width="58" height="24" rx="5" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="773" y="483" text-anchor="middle" font-size="12" font-weight="400" fill="#1c1917">局部和</text>
<text x="702" y="506" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">局部结果 s512</text>
<path d="M802 478 H817" fill="none" stroke="#9a3412" stroke-width="1.4"/>
<text x="702" y="428" text-anchor="middle" font-size="25" font-weight="400" fill="#78716c">⋮</text>
<text x="702" y="538" text-anchor="middle" font-size="12" font-weight="400" fill="#57534e">任务 505—512 各处理一个分区</text>
<path d="M817 286 V562 H702 V574" fill="none" stroke="#9a3412" stroke-width="1.5" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="615" y="574" width="174" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="702" y="593" text-anchor="middle" font-size="14" font-weight="600" fill="#9a3412">机器内合并</text>
<text x="702" y="611" text-anchor="middle" font-size="11" font-weight="400" fill="#9a3412">8 个局部结果 → 1 个结果</text>
<text x="560" y="371" text-anchor="middle" font-size="23" font-weight="400" fill="#78716c">⋯</text>
<path d="M138 620 V642 H83.5 V670" fill="none" stroke="#9a3412" stroke-width="1.5" marker-end="url(#dataflow-sum-example-merge)"/>
<path d="M418 620 V650 H83.5 V670" fill="none" stroke="#9a3412" stroke-width="1.5" marker-end="url(#dataflow-sum-example-merge)"/>
<path d="M702 620 V642 H762.5 V670" fill="none" stroke="#9a3412" stroke-width="1.5" marker-end="url(#dataflow-sum-example-merge)"/>
<text x="420" y="662" text-anchor="middle" font-size="12" font-weight="400" fill="#57534e">64 台机器按每 8 台分组；图中省略其余机器的连线</text>
<rect x="43.0" y="670" width="81" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="83.5" y="690" text-anchor="middle" font-size="11" font-weight="600" fill="#9a3412">合并任务 1</text>
<text x="83.5" y="707" text-anchor="middle" font-size="10" font-weight="400" fill="#57534e">接收 8 机结果</text>
<line x1="83.5" y1="716" x2="350" y2="766" stroke="#9a3412" stroke-width="1.4" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="140.0" y="670" width="81" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="180.5" y="690" text-anchor="middle" font-size="11" font-weight="600" fill="#9a3412">合并任务 2</text>
<text x="180.5" y="707" text-anchor="middle" font-size="10" font-weight="400" fill="#57534e">接收 8 机结果</text>
<line x1="180.5" y1="716" x2="370" y2="766" stroke="#9a3412" stroke-width="1.4" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="237.0" y="670" width="81" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="277.5" y="690" text-anchor="middle" font-size="11" font-weight="600" fill="#9a3412">合并任务 3</text>
<text x="277.5" y="707" text-anchor="middle" font-size="10" font-weight="400" fill="#57534e">接收 8 机结果</text>
<line x1="277.5" y1="716" x2="390" y2="766" stroke="#9a3412" stroke-width="1.4" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="334.0" y="670" width="81" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="374.5" y="690" text-anchor="middle" font-size="11" font-weight="600" fill="#9a3412">合并任务 4</text>
<text x="374.5" y="707" text-anchor="middle" font-size="10" font-weight="400" fill="#57534e">接收 8 机结果</text>
<line x1="374.5" y1="716" x2="410" y2="766" stroke="#9a3412" stroke-width="1.4" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="431.0" y="670" width="81" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="471.5" y="690" text-anchor="middle" font-size="11" font-weight="600" fill="#9a3412">合并任务 5</text>
<text x="471.5" y="707" text-anchor="middle" font-size="10" font-weight="400" fill="#57534e">接收 8 机结果</text>
<line x1="471.5" y1="716" x2="430" y2="766" stroke="#9a3412" stroke-width="1.4" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="528.0" y="670" width="81" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="568.5" y="690" text-anchor="middle" font-size="11" font-weight="600" fill="#9a3412">合并任务 6</text>
<text x="568.5" y="707" text-anchor="middle" font-size="10" font-weight="400" fill="#57534e">接收 8 机结果</text>
<line x1="568.5" y1="716" x2="450" y2="766" stroke="#9a3412" stroke-width="1.4" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="625.0" y="670" width="81" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="665.5" y="690" text-anchor="middle" font-size="11" font-weight="600" fill="#9a3412">合并任务 7</text>
<text x="665.5" y="707" text-anchor="middle" font-size="10" font-weight="400" fill="#57534e">接收 8 机结果</text>
<line x1="665.5" y1="716" x2="470" y2="766" stroke="#9a3412" stroke-width="1.4" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="722.0" y="670" width="81" height="46" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="762.5" y="690" text-anchor="middle" font-size="11" font-weight="600" fill="#9a3412">合并任务 8</text>
<text x="762.5" y="707" text-anchor="middle" font-size="10" font-weight="400" fill="#57534e">接收 8 机结果</text>
<line x1="762.5" y1="716" x2="490" y2="766" stroke="#9a3412" stroke-width="1.4" marker-end="url(#dataflow-sum-example-merge)"/>
<rect x="315" y="766" width="210" height="48" rx="8" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/>
<text x="420" y="786" text-anchor="middle" font-size="15" font-weight="600" fill="#9a3412">最终合并任务</text>
<text x="420" y="805" text-anchor="middle" font-size="12" font-weight="400" fill="#9a3412">合并 8 个结果，输出总和</text>
<text x="420" y="846" text-anchor="middle" font-size="15" font-weight="600" fill="#9a3412">512 个局部结果 → 64 个机器结果 → 8 个分组结果 → 1 个总和</text>
</svg>
<figcaption class="fig-caption">512 个任务并行计算局部和。各机器先合并本机结果。后续任务继续分层合并。最终结果依赖全部输入分区。</figcaption>
</figure>

图的上半部表示算子定义。下半部展开并行任务。**一个算子定义可以对应许多个执行实例。** 引擎把这些实例分配到不同机器。

非聚合查询可以独立处理各个分区。聚合查询还需要合并局部结果。并行任务增加了图的宽度。合并步骤仍然保留了数据依赖。

两张数据流图都没有反馈。它们都是 DAG。本章先讨论这种无环结构。

图确定了数据路径。执行引擎还要决定谁发起下一步处理。

### 3.2 拉取执行与推送执行

**拉取执行（pull）：下游向上游请求数据。** 输出端先向投影算子取数。请求沿算子链逐级传到扫描算子。扫描算子读取记录。记录再沿数据路径返回下游。

**推送执行（push）：上游把数据交给下游。** 扫描算子把读到的记录交给过滤算子。过滤算子把合格记录交给投影算子。投影算子再把结果交给输出端。

两种方式都能执行同一张数据流图。它们决定谁发起处理。引擎另行决定线程和任务的分配。

### 3.3 火山模型：用迭代器连接算子

火山模型（Volcano model）是拉取执行的典型实现。每个算子提供三个接口：

- `open()` 初始化执行状态。
- `next()` 返回下一条结果。结果耗尽时，`next()` 报告结束。
- `close()` 释放执行资源。

输出端调用投影算子的 `next()`。投影算子向过滤算子取数。过滤算子持续向扫描算子取数。它只返回满足条件的记录。

调用从下游走向上游。记录从上游返回下游。一个线程就能沿调用栈执行这条算子链。下游通过同步调用链请求数据。下游停止取数后，上游就停止生产。

引擎可以通过统一接口替换算子实现。数据库引擎常用这套接口执行 SQL。接口本身不限定算子的计算内容。[Volcano 原论文](https://awoc.wolski.fi/dlib/parallel-query/Graefe94-Volcano.pdf)也明确讨论了对不同数据模型的支持。

### 3.4 推送模型：由上游驱动计算

推送模型（push-based）由上游驱动执行。引擎从扫描算子启动处理。下游算子提供接收记录的操作。这里用 `consume(record)` 表示这个操作。

继续使用第 3.1 节的非聚合查询：

```text
扫描算子：
  逐条读取订单
  调用过滤算子.consume(订单)

过滤算子.consume(订单)：
  如果订单金额不低于 100：
    调用投影算子.consume(订单)

投影算子.consume(订单)：
  输出订单号
```

上游算子触发下游算子的处理。记录也沿相同方向传递。一个线程就能完成这条调用链。

对于前面的聚合查询，聚合算子逐条更新累计值。输入结束时，上游发送结束通知。聚合算子随后输出最终汇总值。

引擎可以通过函数调用实现 `consume(record)`。编译型引擎还可以把这条算子链合入同一段循环。HyPer 采用了这种推送式处理。参见 [Neumann 的查询编译论文](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf)。

### 3.5 流水线与流水线阻断点

拉取执行和推送执行都可以形成流水线。

扫描、过滤和投影通常可以逐条传递记录。引擎可以把相邻的这些算子组成一条**流水线（pipeline）**。流水线内部不必保存整份中间结果。

有些算子必须先准备足够的输入。全量排序算子先收齐输入。随后，排序算子开始输出有序记录。常规哈希连接算子先构建哈希表。随后，哈希连接算子开始探测。这些等待边界形成**流水线阻断点（pipeline breaker）**。

引擎在流水线阻断点保存中间数据。这个过程称为物化。后续阶段读取物化后的数据。引擎可以在内存或磁盘中保存数据。

下面的执行计划包含连接和排序。哈希连接算子先关联订单与客户。排序算子按客户排列关联结果。分组汇总算子再逐组计算订单总额。

<figure class="fig-card fig-card--dense">
<svg class="fig-svg" viewBox="0 0 800 440" role="img" aria-label="左：构建哈希表与排序 形成流水线阻断点；右：三条流水线通过哈希表与有序数据交接，编号表示执行阶段">
<defs>
<marker id="fig1-idle" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#a8a29e"/></marker>
<marker id="fig1-orange" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M2 1.5 L9 5 L2 8.5 Z" fill="#9a3412"/></marker>
</defs>
<text x="195" y="28" text-anchor="middle" class="t-title" fill="#9a3412">算子树：阻断点划分流水线</text>
<text x="195" y="46" text-anchor="middle" class="t-sub">数据自下而上传递，在阻断点保存</text>
<text x="592" y="28" text-anchor="middle" class="t-title" fill="#9a3412">执行过程：三条流水线依次推进</text>
<text x="592" y="46" text-anchor="middle" class="t-sub">哈希表与有序数据连接各个阶段</text>
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
<rect x="147" y="64" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="195" y="85" text-anchor="middle" class="t-label">分组汇总</text>
<rect x="147" y="132" width="96" height="34" rx="9" fill="#ffedd5" stroke="#9a3412" stroke-width="1.5"/><text x="195" y="153" text-anchor="middle" class="t-label">排序</text>
<rect x="147" y="200" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="195" y="221" text-anchor="middle" class="t-label">哈希连接</text>
<rect x="87" y="268" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="135" y="289" text-anchor="middle" class="t-label">过滤</text>
<rect x="237" y="268" width="96" height="34" rx="9" fill="#ffedd5" stroke="#9a3412" stroke-width="1.5"/><text x="285" y="289" text-anchor="middle" class="t-label">构建哈希表</text>
<rect x="87" y="336" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="135" y="357" text-anchor="middle" class="t-label">扫描订单</text>
<rect x="237" y="336" width="96" height="34" rx="9" fill="#ffffff" stroke="#d6d3d1" stroke-width="1.4"/><text x="285" y="357" text-anchor="middle" class="t-label">扫描客户</text>
</g>
<g stroke="#9a3412" stroke-width="2" stroke-dasharray="7 5" fill="none">
<line x1="135" y1="115" x2="255" y2="115"/>
<line x1="256" y1="264" x2="277" y2="238"/>
</g>
<text x="263" y="119" class="t-micro" fill="#9a3412">物化：有序数据</text>
<text x="290" y="242" class="t-micro" fill="#9a3412">物化：哈希表</text>
<rect x="30" y="398" width="16" height="16" rx="4" fill="#ffedd5" stroke="#9a3412" stroke-width="1.2"/>
<text x="54" y="411" class="t-sub">阻断点：引擎在此保存数据</text>
<g>
<rect x="405" y="64" width="375" height="64" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="417" y="82" class="t-micro" fill="#9a3412" font-weight="700">P1 · 阶段 1</text>
<rect x="417" y="90" width="82" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="458" y="107" text-anchor="middle" class="t-label">扫描客户</text>
<line x1="499" y1="103" x2="509" y2="103" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="511" y="90" width="96" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="559" y="107" text-anchor="middle" class="t-label">构建哈希表</text>
<line x1="607" y1="103" x2="617" y2="103" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="619" y="90" width="124" height="26" rx="7" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/><text x="681" y="107" text-anchor="middle" class="t-label">哈希表 · 物化</text>
<rect x="405" y="184" width="375" height="64" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="417" y="202" class="t-micro" fill="#9a3412" font-weight="700">P2 · 阶段 2</text>
<rect x="417" y="210" width="88" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="461" y="227" text-anchor="middle" class="t-label">扫描订单</text>
<line x1="505" y1="223" x2="515" y2="223" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="517" y="210" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="549" y="227" text-anchor="middle" class="t-label">过滤</text>
<line x1="581" y1="223" x2="591" y2="223" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="593" y="210" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="625" y="227" text-anchor="middle" class="t-label">探测连接</text>
<line x1="657" y1="223" x2="667" y2="223" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="669" y="210" width="98" height="26" rx="7" fill="#ffedd5" stroke="#9a3412" stroke-width="1.3"/><text x="718" y="227" text-anchor="middle" class="t-label">有序数据·物化</text>
<rect x="405" y="304" width="375" height="64" rx="10" fill="#f6f1e7" stroke="#e8e0d4"/>
<text x="417" y="322" class="t-micro" fill="#9a3412" font-weight="700">P3 · 阶段 3</text>
<rect x="417" y="330" width="104" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="469" y="347" text-anchor="middle" class="t-label">读回有序数据</text>
<line x1="521" y1="343" x2="531" y2="343" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="533" y="330" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="565" y="347" text-anchor="middle" class="t-label">分组汇总</text>
<line x1="597" y1="343" x2="607" y2="343" stroke="#a8a29e" stroke-width="1.5" marker-end="url(#fig1-idle)"/>
<rect x="609" y="330" width="64" height="26" rx="7" fill="#ffffff" stroke="#d6d3d1"/><text x="641" y="347" text-anchor="middle" class="t-label">输出</text>
</g>
<g stroke="#9a3412" stroke-width="1.8" stroke-dasharray="6 4" fill="none">
<line x1="681" y1="120" x2="625" y2="209" marker-end="url(#fig1-orange)"/>
<path d="M 718 240 L 718 274 L 469 274 L 469 329" marker-end="url(#fig1-orange)"/>
</g>
<text x="694" y="160" class="t-micro" fill="#9a3412">探测前必须先建好</text>
<text x="594" y="268" text-anchor="middle" class="t-micro" fill="#9a3412">交给 P3 扫描读回</text>
<g stroke="#9a3412" stroke-width="1.5" fill="none">
<line x1="405" y1="388" x2="780" y2="388"/>
<line x1="405" y1="382" x2="405" y2="394"/>
<line x1="780" y1="382" x2="780" y2="394"/>
</g>
<text x="592" y="410" text-anchor="middle" class="t-sub">三条流水线属于同一次查询，引擎负责分配线程</text>
</svg>
<figcaption class="fig-caption">左：橙色节点形成流水线阻断点。右：P1 构建哈希表。P2 先完成连接。P2 再完成排序。P3 读取有序数据。它逐组汇总订单金额。哈希表和有序数据连接三条流水线。引擎决定线程分配。</figcaption>
</figure>

引擎可以用一个线程执行一条流水线。引擎也可以为不同分区创建流水线实例。流水线的划分并不直接规定线程数量。

### 3.6 数据分区与并行执行

执行引擎还需要把工作分给多个任务。对于扫描、过滤和投影，引擎可以先划分输入。它为每个分区创建任务。不同任务便能并行处理各自的记录。

连接和聚合可能还需要重新分区。引擎需要把相关记录送到同一任务。Volcano 系统使用 **Exchange 算子**处理进程之间的数据传输。Exchange 算子也支持数据分区。

进程内的下游算子通过 `next()` 拉取数据。发送进程向接收进程推送数据。Exchange 算子连接这两种执行方式。它把数据处理逻辑与进程间通信分开。参见 [Volcano 的并行执行设计](https://awoc.wolski.fi/dlib/parallel-query/Graefe94-Volcano.pdf)。

Exchange 算子划出通信边界。它可以持续传送已经产生的记录。通信边界因此不一定形成流水线阻断点。

Flink 的算子链也体现了这种分工。Flink 可以用同一任务线程执行多个算子。不同任务通过通道传递数据。参见 [Flink 的任务与算子链](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/concepts/flink-architecture/#tasks-and-operator-chains)。

DAG 规定任务需要等待哪些输入。引擎决定任务使用哪些线程和机器。增加机器无法消除任务之间的依赖。


## 4. 从静态计算图到动态计算图

### 4.1 静态图固定算子的表达能力

第三章说明了算子怎样执行。接下来，我们讨论这些算子能够表达什么。

静态计算图预先规定节点与依赖。这个定义只约束图的结构。它没有限制节点内部的计算。一个节点也可以封装完整的递归程序。

要讨论表达边界，我们先限定一种算子语言。它采用集合语义。它只包含经典关系代数运算：选择、投影、连接、并、差等。查询用固定、有限的表达式组合这些运算。输入规模不会改变表达式的结构。

**经典关系代数与域独立的一阶关系演算等价。** 两者能够表达同一类查询。这里的关系代数表达式都是固定、有限的。参见《数据库基础》[定理 5.3.10](https://webdam.di.ens.fr/Alice/pdfs/Chapter-5.pdf)。

**一阶关系查询按固定的表达式结构计算。每层运算产生中间结果。后续运算使用这些结果。**

**一阶关系查询不提供递归机制。** 这里的递归机制允许程序重复应用同一套规则。程序把推导结果送回规则。程序再次应用规则。结果稳定后，程序停止。

上述等价关系只覆盖非递归 SQL 中的经典关系代数部分。现代 SQL 还包含聚合、重复行和自定义函数等特性。我们需要分别分析这些特性。

改用拉取执行或推送执行，不会改变这组运算的表达能力。火山模型提供了执行接口。算子及其组合规则决定查询能够表达什么。

### 4.2 有限组合在哪里遇到边界

假设一张表记录公司之间的直接持股关系。一次自连接可以找到两层持股关系。再连接一次，可以找到三层关系。

现在，查询要求找出所有间接关系。公司之间可能隔着任意多层。一个预先写定的连接链只能覆盖有限层数。对任意大小的输入，经典关系代数无法表达一般的可达性查询。参见《数据库基础》[第十二章](https://webdam.di.ens.fr/Alice/pdfs/Chapter-12.pdf)。

程序需要一种重复应用规则的方式。程序每轮根据已有关系推导新关系。程序把新关系加入结果。没有新关系时，计算到达不动点。

已知层数上限时，程序可以提前展开有限轮计算。这会得到针对该上限的表达式。递归查询可以保持同一份定义。程序根据每轮结果决定是否继续。

### 4.3 动态计算图怎样展开

第一章的前缀和算法按跨度 1、2、4 依次合并。输入规模确定后，轮数也随之确定。程序可以提前展开全部轮次。这些轮次构成静态计算图。

对于上面的可达性算法，程序会检查每轮结果。若本轮产生新关系，程序就执行下一轮。我们也可以随着执行逐步展开依赖：

```text
第 1 轮计算 ──结果 1──→ 第 2 轮计算 ──结果 2──→ …
```

每个节点表示某一轮的执行。每条边表示轮次之间的数据依赖。本系列把这种随执行逐步展开的图称为**动态计算图**。这里的节点表示执行实例。程序可以在不同轮次复用同一组算子。

引擎也可以用带反馈边的静态算子图描述同一计算。反馈边把本轮结果送回下一轮。我们可以分别画出每轮执行。各轮之间的依赖仍然向前延伸。有限次迭代会展开成有限的 DAG。

因此，静态或动态描述节点与依赖何时确定。有环或无环描述图的依赖结构。讨论时还要说明节点表示算子，还是一次执行。

### 4.4 下一篇：循环与持续输入

循环增加了轮次之间的依赖。持续输入意味着输入源不断提供新数据。引擎也能用固定的无环算子图处理持续输入。这是两个需要分别讨论的问题。

[第二篇：循环与无界计算]({{ '/2026/10/28/stream-lecture-2-loops-and-unbounded-computation/' | relative_url }})从持股查询开始。它先讨论轮次与不动点。随后，它引入逻辑时间。它用 capability 和 frontier 判断计算进度。最后，它追踪持续新增与删除怎样改变循环结果。


## 延伸阅读

- R. P. Brent, "The Parallel Evaluation of General Arithmetic Expressions" (JACM 1974)：工作量、关键路径与处理器数量的关系。
- Blelloch, "Prefix Sums and Their Applications"：扫描算法的深度与工作量权衡。
- [Goetz Graefe, Volcano—An Extensible and Parallel Query Evaluation System](https://awoc.wolski.fi/dlib/parallel-query/Graefe94-Volcano.pdf) (IEEE TKDE 1994)：迭代器接口与 exchange 的并行扩展。
- [配套 slides：从数据库看流计算]({{ '/slides/flink-for-db-developers.html' | relative_url }})：系列补充材料。
