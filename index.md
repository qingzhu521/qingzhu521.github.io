---
layout: home
---

<style>
  .dbsp-feature { margin: 0 0 2rem; padding: 1.25rem 1.5rem; border-left: 4px solid #9a3412; background: #faf6ef; color: #1c1917; }
  .dbsp-feature h2 { margin: 0 0 .4rem; font-size: 1.5rem; font-weight: 650; line-height: 1.35; }
  .dbsp-feature p { margin: 0 0 .65rem; }
  .dbsp-feature a { color: #83401e; font-weight: 650; text-decoration: underline; text-underline-offset: .18em; }
</style>

<section class="dbsp-feature" aria-labelledby="dbsp-title">
  <h2 id="dbsp-title">DBSP 教程 · 从变化流到增量运行时</h2>
  <p>这套教程由我的 OceanBase（OB）同事<strong>李金懋</strong>制作。</p>
  <p>数据库里的一行变了，查询结果会怎样变？这 12 课从一次插入和一次删除讲起，带你逐步推导 Join、去重、聚合和递归查询的增量计算过程。</p>
  <a href="{{ '/dbsp-ivm/' | relative_url }}">查看 12 课目录 <span aria-hidden="true">→</span></a>
</section>

> 🎞 配套分享 slides：[《从数据库看流计算 —— 写给数据库开发者的 Flink 入门》]({{ '/slides/flink-for-db-developers.html' | relative_url }})，基于「流计算基础」系列提炼的 18 页网页幻灯片，浏览器打开即可放映（方向键 / 空格翻页）。

## 流计算基础

1. [从并行计算到数据流：依赖、DAG 与执行]({{ '/2026/07/27/stream-lecture-1-dag-and-loop/' | relative_url }})
2. [循环与无界计算：逻辑时间、进度与结果完整性]({{ '/2026/10/28/stream-lecture-2-loops-and-unbounded-computation/' | relative_url }})
3. [事件时间、状态与故障恢复]({{ '/2026/08/05/stream-lecture-2-time-state-and-fault-tolerance/' | relative_url }})
4. [循环中的增量计算]({{ '/2026/11/09/stream-lecture-4-incremental-computation-in-loops/' | relative_url }})
