---
layout: home
---

<style>
  .dbsp-reading { margin: 0 0 .75rem 1.875rem; padding: .65rem 1rem; border-left: 3px solid #9a3412; background: #faf6ef; }
  .dbsp-reading p { margin: 0; }
  .dbsp-reading p + p { margin-top: .25rem; }
</style>

> 🎞 配套分享 slides：[《从数据库看流计算 —— 写给数据库开发者的 Flink 入门》]({{ '/slides/flink-for-db-developers.html' | relative_url }})，基于「流计算基础」系列提炼的 18 页网页幻灯片，浏览器打开即可放映（方向键 / 空格翻页）。

## 流计算基础

1. [从并行计算到数据流：依赖、DAG 与执行]({{ '/2026/07/27/stream-lecture-1-dag-and-loop/' | relative_url }})
2. [循环与无界计算：逻辑时间、进度与结果完整性]({{ '/2026/10/28/stream-lecture-2-loops-and-unbounded-computation/' | relative_url }})
3. [事件时间、状态与故障恢复]({{ '/2026/08/05/stream-lecture-2-time-state-and-fault-tolerance/' | relative_url }})

<aside class="dbsp-reading" aria-label="第四篇的配套阅读">
  <p><strong>第四篇的配套阅读</strong> · <a href="{{ '/dbsp-ivm/' | relative_url }}">DBSP 教程《从变化流到增量运行时》</a></p>
  <p>这套教程由我的 OceanBase（OB）同事<strong>李金懋</strong>制作，共 12 课。第 7 课讲去重，第 11 课讲递归。</p>
</aside>

<ol start="4">
  <li><a href="{{ '/2026/11/09/stream-lecture-4-incremental-computation-in-loops/' | relative_url }}">循环中的增量计算</a></li>
</ol>
