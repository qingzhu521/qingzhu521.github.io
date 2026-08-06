---
layout: post
title: "AI 语言笑话录"
date: 2026-08-06 12:00:00 +0800
categories: meta
tags: [ai, chinese, writing, meta]
---

<style>
.post-content {
  font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Helvetica Neue", "Segoe UI", "Microsoft YaHei", sans-serif;
  font-size: 1.0625rem;
  line-height: 1.85;
}
.post-content h2 { font-size: 1.5rem; font-weight: 700; letter-spacing: -0.01em; margin-top: 2.4em; }
.post-content table { border-collapse: collapse; width: 100%; font-size: 0.9375rem; line-height: 1.6; }
.post-content th { background: #f6f1e7; font-weight: 600; padding: 10px 12px; text-align: left; border-bottom: 1px solid #e8e0d4; }
.post-content td { padding: 10px 12px; border-bottom: 1px solid #efe8db; vertical-align: top; }
.post-content details { margin: 24px 0; padding: 16px 20px; background: #fffdf8; border: 1px solid #e8e0d4; border-radius: 14px; }
.post-content details summary { cursor: pointer; font-weight: 600; color: #0f766e; }
.post-content blockquote { border-left: 4px solid #d6d3d1; margin: 16px 0; padding: 4px 16px; color: #57534e; }
</style>

这份笑话录有真实的出处：一篇流计算技术博客的修改过程。我和一个 AI 助手一起写中文技术文章，改着改着发现，AI 写中文有一个稳定的病理：**语法全对，结构工整，但就是不像人话**。它的病不是错别字，而是选词错位、比喻强行、对仗硬凑、术语乱译、把自己当文献综述。以下十句，全部来自真实修改现场，每条附上人类改法。

## 1. 「固定下来」——人类说"展示在这里"

> 在动笔之前，先把全篇唯一的例子**固定下来**。

"固定下来"是工程腔。数据既不是螺丝，也不是制度，没有人会对同事说"先把例子固定下来"。人类说：**先把例子展示在这里**。

## 2. 「从拍脑袋到精确账目」——硬凑的对仗

> 四种进度机制：从**拍脑袋**到**精确账目**

"拍脑袋"是活的口语，"精确账目"是死的书面语，中间塞一个"到"，就是典型的 AI 对仗——两边字数一样，气质完全不通。这不是修辞，是排版。人类改法：**逐渐严谨的进度追踪**。四个机制的证据强度确实是递增的，说人话就好，不用凑对子。

## 3. 「DAG 一画到底」——强行比喻

> 数据已经在场，有界输入，DAG **一画到底**，算完即止。

"一画到底"通常形容一笔连写，用在 DAG 上是 AI 式强行比喻：看起来有画面，其实没人这么说话。而且它和同一段的"依赖关系可以画成图"重复——同一个意象说两遍，是为了显得"有文采"，结果只是冗余。人类改法：删掉。

## 4. 「入口处修好 vs 到达即算」——错误翻译不如不翻译

> 两种架构直觉：**入口处修好** vs **到达即算**

这是 AI 给 in-order / out-of-order 造的中文译名。in-order 的要点是"入口缓冲重排、下游见到有序流"，"修好"没有宾语，读者还得猜修好什么；而且它暗示系统能把顺序修到完美，实际有迟到上界，越界直接丢。翻译不到位，比不翻译更害人。人类改法：**in-order vs out-of-order**，中文只作破折号后的描述，不作术语。

## 5. 「数据已经在场」——"在场"是人的词

> 数据已经在**场**

"在场"用于人：观众在场，证人到场。数据"在场"，是把数据拟人化还自以为生动。人类改法：**数据已经到齐**。"到齐"才是描述输入完整性的母语。

## 6. 「完整性是免费的」——隐喻错位

> 在有界世界里，完整性是**免费**的。

"免费"暗示"本来要钱、现在不要"，完整性和钱没有关系。这是 AI 从"for free"直译过来的廉价感隐喻。人类改法：**完整性是不言自明的**（或"与生俱来"）。文件读到结尾就完整，这是天性，不是折扣。

## 7. 「环环相扣」——过誉的总结词

> 会牵出六个**环环相扣**的追问

六个问题真的环环相扣吗？不是。它们是两条线：四个从"数据不到齐"引出，两个要等"机器会坏"才讲得透，中间还有一个共用的地基和一个汇合点。AI 爱用"环环相扣""层层递进""息息相关"这类现成的四字总结词，因为它们看起来像结论，其实只是把结构糊住了。人类改法：把真实结构说出来。

## 8. 「综述第 X.Y 节用 Figure 2……」——读书报告腔

> 综述第 3.5.1 节用一张图（Figure 2）把四种机制放在同一个窗口计数任务上对比。下面**照搬**这个方法……

写技术文章最怕把自己写成文献综述：每一段都挂"综述第几节说""Table 几把某系统归为"。读者读的是你的理解，不是你的文献索引。AI 这么写，是因为它真的在"转述"，而人类写作的要求是"讲明白"。改法：把机制讲清楚，把综述降级成延伸阅读。

## 9. 「四代议题」——术语撞车

> 综述把流系统的演化分成**四代议题**：乱序管理（第 3 章）、状态管理（第 4 章）……

"四代"和"第一代、第二代"是两个体系——前者是功能分类，后者是系统代际。AI 把"四个功能支柱"写成"四代议题"，读者会以为乱序管理是一代系统、状态管理是下一代。同文不同义是最隐蔽的错。人类改法：**四个功能支柱**。

## 10. 「这句话不能直接写进正文」——AI 在文章里跟自己吵架

初版里有两句最传神的：

> 因此"每个元素都有 Event Time、Ingestion Time、Processing Time 三个属性"这句话**不能直接写进正文**。

> 这一代架构以 scale-up 和有序输入为主，但"以单机为主"**不能写成"全部是单机"**。

AI 一边写稿一边自我审查，审查意见还留在正文里。人类审稿是改完删掉痕迹；AI 审稿是把自己的心虚也一起发布。这两句是整本笑话录的灵魂。

## 判断标准

如果你也想拿这份清单当镜子，标准只有一条：**这句话，你会对同事说吗？** 技术写作首先是说话，说人话，然后才是排版。

---

## 附录：笑话的出处

以下是一篇技术博客的 AI 初稿《流计算基础（二）：时间、状态与正确性 —— 从 Watermark 到 Checkpoint》全文（正文部分，样式与图略）。上面十条里，第五条、第八条、第九条、第十条都能在原文里找到原句。它语法正确、结构完整、逻辑自洽——只是不像人写的。

<details markdown="1">
<summary>点击展开：AI 初稿全文</summary>

第一篇讨论的是一个近乎理想的计算世界：数据已经在场，依赖关系可以画成 DAG；需要迭代时，再加一条回边和一套进度协议。那个世界里没有网络延迟，没有机器宕机，也没有一条本该属于十分钟前的数据在结果发布以后才姗姗来迟。

真实世界偏偏由这些例外组成。手机离线、消息队列积压、分区负载不均、进程重启，都会让"事情发生的顺序"和"系统看到它们的顺序"分离。数据又没有终点：如果计算必须等全部输入到齐，它将永远无法输出；如果来一条算一条，它又无法知道一扇窗口、一笔 join 或一次去重何时可以结束。

要把流计算与流式状态说清楚，必须先从时间模型开始。时间模型不是为了给记录多加几个字段，而是为了做**归因**：一笔交易属于哪个小时，一次修改属于哪个版本，一条消息应当改变哪一份结果。人类世界通常相信，未来发生的事情不应改写已经封账的过去；流系统必须把这条直觉变成一个可以执行的边界。

本文围绕两个问题展开：系统凭什么宣布"过去已经完整"，以及宣布之后如果机器失败或数据反悔，怎样仍然得到正确结果。

## 1. 为什么计算需要时间：先决定一条数据属于哪里

假设一笔订单在用户手机上完成支付：

```text
10:03  用户完成支付
10:17  手机恢复网络，订单进入消息系统
10:18  流计算算子处理这条订单
次日   作业故障恢复，再次处理同一条订单
```

这条订单至少牵涉三种时间。

| 时间 | 示例 | 回答的问题 |
| --- | --- | --- |
| Event Time | 10:03 | 业务事实什么时候发生？它应归入哪个结果？ |
| Ingestion Time | 10:17 | 数据什么时候进入流处理系统？入口积压了多久？ |
| Processing Time | 10:18，重放时可能是次日 | 某个算子、某次执行实际什么时候处理它？ |

这三个时间是三种语义，不一定是记录上三个固定字段。Event Time 通常来自业务字段或 source 分配的时间戳；Ingestion Time 可以在入口观测并记录一次；Processing Time 是算子运行时读取的本地时钟，同一条数据经过不同算子或故障重放时会得到不同值。

因此"每个元素都有 Event Time、Ingestion Time、Processing Time 三个属性"这句话不能直接写进正文。流处理文献把应用分成三种时间域；早期 Flink 也曾把三者暴露为全局 `TimeCharacteristic`。从 Flink 1.12 开始，这个全局开关被弃用：Event Time 和 Processing Time 由具体窗口、timer 和 `WatermarkStrategy` 显式表达，原先的 Ingestion Time 用例则应在 source 处分配时间戳并选择合适的 WatermarkStrategy。三分法仍然有分析价值，但它不是当前 Flink 强制附加在每条记录上的三个物理字段。

如果统计"10 点到 11 点的成交额"，应使用 10:03；如果监控手机到平台的上报延迟，应比较 10:03 与 10:17；如果分析算子排队和反压，应观察 10:17 以后各阶段的 Processing Time。把三个时间混在一起，故障重放就可能把昨天的订单算进今天，网络抖动也可能被误认为业务量突增。

因此时间模型的第一项职责不是排序，而是归因。它定义结果的前缀：

```text
R(T) = F({ e | EventTime(e) <= T })
```

如果一个结果声称自己代表时间 \(T\) 以前的世界，它就必须回答：还有没有 Event Time 不晚于 \(T\) 的数据正在路上？

## 2. 为什么要限定顺序：不是所有数据都需要排成一条队

"严格顺序"容易让人想到全局排序，但流系统通常不需要把世界上所有事件排成唯一序列。它只需要建立足以保证当前计算正确的偏序。

`map`、`filter`、`project` 逐元素工作。先处理 A 还是 B，通常不影响彼此，结果可以立即输出。

聚合、join 和去重不同：

- 窗口聚合必须知道还有没有属于这个窗口的数据。
- 内连接可以在匹配发生时增量输出，但外连接想输出"没有匹配"的一侧，必须先证明另一侧以后也不会再来。
- 去重必须记住某个 key 是否出现过；如果没有时间边界，"见过的 key"集合会无限增长。
- 排序、Top-N、会话窗口都需要知道哪些未来输入仍可能改变当前答案。

这里真正需要的不是"所有记录严格按时间到达"，而是一个**可关闭的前缀**：系统能证明某个时间、版本或迭代之前的工作不会再被正常输入改变，于是可以输出、清理状态并继续向前。

在有限批处理中，输入结束天然提供这个证明；在无界流中，"文件结尾"永远不会出现，系统必须自己制造边界。这带来六个无法绕开的问题：

1. 无限数据怎样被切成可计算的有限部分？
2. 哪些历史数据必须作为状态保留？
3. 什么时候可以认为某部分结果完整？
4. 数据乱序、迟到时怎么办？
5. 机器故障后怎样恢复同一份结果？
6. 输入被插入、删除或更正后，已发布结果怎样更新？

后面的 Watermark、Frontier、Revision 与 Checkpoint，分别回答这些问题的不同部分。

## 3. 一段很短的历史：从连续查询到分布式数据流

第一代流处理系统主要来自数据库研究。STREAM、TelegraphCQ、Aurora/Borealis 等系统建立了连续查询、窗口、流关系模型、负载管理和早期容错机制。这一代架构以 scale-up 和有序输入为主，但"以单机为主"不能写成"全部是单机"：Borealis 等工作已经探索了分布式、高可用与动态修订。

MapReduce 与云计算普及以后，关注点转向 shared-nothing 集群上的数据并行。Storm、MillWheel、Spark Streaming、Flink 等系统让用户通过 API 组合数据流图，运行时负责分区、调度、故障恢复和扩缩容。这里也不应说 MapReduce 本身提供了流式计算；准确说，是 MapReduce 推广了 commodity cluster 上的 scale-out 数据流执行方式，第二代流系统把这条路线带进了无界数据。

两代系统的差别不是本文重点。重要的是，分布式化放大了两个原本就存在的问题：不同分区不再同步到达，运行数月的状态也不能在机器故障后从头计算。时间进度与状态恢复因此从查询细节变成系统地基。

## 4. Watermark：给无界输入制造一个"暂时的结尾"

Watermark 可以先用一句话理解：

> `Watermark(T)` 表示系统认为，未来正常到达的数据不会再包含 Event Time 不晚于 `T` 的记录。

这个声明可能来自源端承诺，也可能来自观测最大 Event Time 后减去一段 slack 的启发式估计。它不意味着墙上时间已经走到 `T`，也不意味着数据绝对不会迟到；它意味着系统愿意据此采取行动。

一个窗口算子收到 Watermark 后可以：

- 触发结束时间已经越过的窗口；
- 将后来的旧时间戳记录标记为 late；
- 清理不再需要的窗口、join 或去重状态；
- 触发 Event Time timer。

Watermark 解决的是"语义上的过去完整到哪里"。它不负责证明某条消息已经成功处理，也不负责机器重启；那是 ack、snapshot 和 checkpoint 的问题。

## 5. Storm、Flink 与 Timely：三种不同的进度观

### 5.1 Storm：先解决"消息树完成了吗"

经典 Storm 的核心突破不是 Event-Time Watermark，而是对 tuple tree 的完成检测。一条 spout tuple 可能在下游展开成成千上万条 tuple；acker 不保存整棵树，只维护所有创建和确认 tuple id 的 XOR。XOR 回到 0，表示这棵 tuple tree 已经完成。

在可靠 spout、anchoring 和 ack 都正确使用时，tuple 超时会触发 replay，从而提供 at-least-once。关闭 ack/replay 后，失败可能造成丢失，这通常被描述为 best effort 或 at-most-once；不是 XOR 同时"实现了 at-most-once"。Exactly-once 则属于 Trident 的微批事务模型。

这套协议回答：

```text
从某条源消息展开的所有工作，是否已经被确认？
```

它不回答：

```text
Event Time 12:00 以前的数据是否都到齐？
```

Storm 后来的 Windowing API 才加入 Event Time 与 Watermark：对每条输入观察最新 tuple timestamp，减去配置的 lag，再在多输入之间取最小值并周期性触发窗口。这个生成方式本质上也是 slack-based watermark，但它与 Storm 的 XOR ack 是两套正交机制。

### 5.2 Flink：每一路报告进度，下游取最保守值

Flink 常见的 bounded-out-of-orderness 策略，在每个 source split/partition 上维护：

```text
LocalWM_i = MaxEventTime_i - B
```

其中 \(B\) 是允许的乱序幅度。算子对所有活跃输入通道取最小值：

```text
OperatorWM = min(LocalWM_1, ..., LocalWM_n)
```

例如：

```text
P0：max ET=12:30，local WM=12:20
P1：max ET=12:18，local WM=12:08

下游 currentInputWatermark=min(12:20, 12:08)=12:08
```

Watermark 作为控制消息沿数据流传播；同一通道里，它不会越过排在自己前面的数据。一个输入停住就会拖住下游，因此 Flink 还需要 idleness 将长期无数据的分区暂时排除。

这里不存在放在中心节点里的"全作业唯一 Global Watermark"。更准确地说，每个算子实例都根据自己的活跃输入计算当前 Watermark，时间进度逐层向下游传播。

### 5.3 Timely：跟踪未来仍可能出现哪些逻辑时间

Timely Dataflow 的时间通常不是业务墙上时间，而是逻辑时间，例如：

```text
(epoch, iteration)
```

Naiad 进一步把逻辑时间与数据流位置组合成 pointstamp：

```text
pointstamp = (timestamp, location)
```

系统跟踪未完成消息和仍被算子持有的时间 capability，由此计算 frontier。Frontier 不是一个启发式"等十分钟"，而是一项结构化保证：按照当前 capability、未完成消息和图路径，未来工作只能出现在 frontier 或它之后。多维时间使用偏序，所以 frontier 可能是一组互不可比的最小时间，而不是一个标量。

这套模型特别适合循环。消息每经过 feedback，iteration 增加；系统不需要等一轮全局屏障，只需判断是否仍有 pointstamp 能产生某个逻辑时间的工作。

但"Timely 不需要迟到策略"只在一个限定范围内成立：对于已经纳入 Timely 逻辑时间和 capability 协议的内部计算，frontier 能精确表达进度，不需要再猜一个固定 slack。对于来自手机、Kafka 或数据库 CDC 的外部 Event Time，输入方仍然必须决定何时推进 input frontier。输入一旦宣布旧 epoch 完成，后来对过去业务事实的更正就必须作为更晚逻辑时间上的撤回/更新表达，不能假装早先的完成声明没有发生。

| 维度 | Storm | Flink | Timely/Naiad |
| --- | --- | --- | --- |
| 核心进度问题 | tuple tree 是否处理完成 | Event Time 推进到哪里 | 未来还可能出现哪些逻辑时间的工作 |
| 主要机制 | XOR ack；Windowing 另有 watermark | Watermark 控制消息，输入取 min | timestamp、capability、pointstamp、frontier |
| 时间 | Windowing 可使用 Event Time | Event Time 为主，也支持 Processing Time timer | 通用逻辑时间，可编码 epoch/iteration/Event Time |
| 循环 | 没有通用的偏序进度模型 | 普通 watermark 在反馈环中容易停滞 | Frontier 原生处理嵌套与循环 |
| 故障恢复 | 超时 replay；Trident 提供事务语义 | Checkpoint + source replay | Naiad 另有一致 checkpoint；与 frontier 正交 |

## 6. Watermark 之后还能不能改：Allowed Lateness 与 Revision

假设窗口是：

```text
[10:00, 10:10)
```

当算子 Watermark 越过 10:10，窗口第一次触发：

```text
GMV=100
```

这时来了一笔 Event Time 为 10:08 的订单，它已经晚于 Watermark。系统有三个选择：丢弃、送入 late-data side output，或者保留窗口状态并修订结果。

在 Flink 中，`allowedLateness(5 minutes)` 的典型含义不是"再等五分钟墙上时间才第一次输出"，而是：窗口在 Watermark 越过 10:10 时照常触发，但状态继续保留，直到 Watermark 进一步越过 10:15。期间到达的迟到记录仍可进入窗口并产生一次 late firing：

```text
第一次输出：GMV=100
迟到订单到达
修订输出：  GMV=120
```

这里有两个不同参数：

| 参数 | 控制什么 |
| --- | --- |
| Out-of-orderness / slack | Watermark 相对已观察 Event Time 落后多少 |
| Allowed lateness | 窗口首次触发后，状态还保留到多晚的 Watermark |

二者都用 Event-Time 进度衡量，不等于睡眠一段墙上时间。Spark Structured Streaming 的 `withWatermark(eventTime, delayThreshold)` 将进度估计与状态清理阈值组合在一个 API 中，不能与 Flink 的 `allowedLateness` 逐字对应。

为什么不能永远允许迟到？因为"允许修改过去"不是一句语义声明，而是一笔不断增长的存储与传播成本：

- 窗口必须继续保存 accumulator 或原始数据；
- join 必须保留两侧索引；
- distinct 必须保留见过的 key；
- 已发布结果发生变化时，下游聚合、join、缓存和 sink 都可能被连带更新。

Watermark 与 allowed lateness 共同划出一条工程边界：边界以内，系统仍愿意为过去保留修订能力；边界以外，状态可以压缩或删除，结果才真正获得"封账"的含义。

## 7. 从修订走向增量计算：状态是历史影响的物化

迟到数据只是输入变化的一种。订单可能取消，用户信息可能更正，数据库 CDC 会带来 insert、delete 和 update。只要输出已经发布，系统就需要一种修订协议：upsert、retract，或者一般化的差分。

假设旧窗口结果为 100，新结果为 120，可以有三种输出方式：

```text
追加增量：       +20
主键覆盖：       100 -> 120
撤回再插入：     -100, +120
```

如果 sink 只支持 append，它看到两个完整快照 `100` 和 `120` 时无法判断它们应当相加还是覆盖。Revision Processing 因而不仅取决于计算引擎，还取决于下游是否理解更新和撤回。

Differential Dataflow 把这种变化统一表示为：

```text
(data, logical_time, diff)
```

插入一条记录是 `diff=+1`，撤回是 `diff=-1`。状态可以理解为历史差分的积分；arrangement 则是为了让未来差分能够快速查询历史而维护的按 key 索引。

这也解释了为什么 join 需要"流存储"。如果 \(A\) 和 \(B\) 都发生变化：

```text
Delta(A JOIN B)
  = DeltaA JOIN B
  + A JOIN DeltaB
  + DeltaA JOIN DeltaB
```

一条新的 \(\Delta A\) 到达时，系统必须查询已经积累的 \(B\)；一条 \(\Delta B\) 到达时，也必须查询已经积累的 \(A\)。Join 的双线性让每次变化可以增量传播，但并没有让历史凭空消失：历史被物化成两侧 arrangement。

这里也要区分 Timely 与 Differential Dataflow。Timely 提供逻辑时间、消息调度和 frontier；Differential Dataflow 在其上提供 `(data, time, diff)`、增量算子与 arrangement。说"Timely 的 join 每来一条数据都做增量计算"不够准确，双线性增量 join 是 Differential Dataflow 这一层提供的代数与实现。

不同算子的"流存储"并不相同：

| 算子 | 必须记住什么 | 什么时候可能删除 |
| --- | --- | --- |
| 窗口聚合 | 每个 key/window 的 accumulator，必要时还有原始记录 | Watermark 越过窗口结束时间和 allowed lateness |
| 双流 join | 两侧按 join key 组织的历史或未匹配记录 | 时间条件证明另一侧不再可能匹配 |
| distinct | 已出现 key 的集合或近似结构 | TTL/版本边界/业务声明允许忘记时 |
| Differential join | 两侧 arrangement 与尚未 compact 的时间差分 | Frontier 允许合并历史时间差异时 |

因此状态不是一个附属缓存，而是"过去仍可能怎样影响未来"的物化。时间模型决定哪些历史仍有区别，progress frontier 决定这些区别何时可以合并，存储引擎则决定它们放在堆内、RocksDB、LSM-tree 还是远端服务中。

## 8. 解决可计算以后，才轮到容错

Watermark 让窗口可以结束，状态让未来输入可以增量计算。但机器仍会失败。容错需要另一条边界：如果现在重启，系统从哪个输入位置和哪份状态恢复？

### 8.1 Storm：用 ack tree 检测丢失并重放

Storm 的 XOR acker 以很小的固定空间追踪一棵 tuple tree。只要某个分支没有 ack，根 tuple 最终超时，spout 负责 replay。因此 at-least-once 的本质是：不允许沉默地丢失，但恢复可能让一条数据再次影响下游。

如果用户状态放在普通 bolt 内存或外部数据库里，Storm core 的 tuple ack 并不会自动给它制造一个全局一致快照。应用必须自己处理幂等、事务或状态恢复；Trident 则通过微批、批次 id 和事务状态提供更强语义。

### 8.2 Flink：Checkpoint Barrier 切出一致快照

Flink 的 checkpoint coordinator 触发 checkpoint 后，source 记录 Kafka offset 等输入位置，并向各通道插入带编号的 checkpoint barrier。算子在一致切面上保存 keyed/operator state；快照持久化完成后，该 checkpoint 才可以用于恢复。

失败时，作业恢复：

```text
source offsets at checkpoint N
               +
operator states at checkpoint N
               +
replay checkpoint 之后的输入
```

Exactly-once 在这里首先意味着：即使物理记录可能因为 replay 被再次执行，每条记录对 Flink 管理状态的最终影响与无故障执行一致。端到端 exactly-once 还要求 source 可重放，并且 sink 支持事务提交或幂等写入。否则，Flink 内部状态能够恢复一致，已经发送到外部的输出仍可能重复。

Aligned checkpoint 会等待同一算子的各输入 barrier 对齐；at-least-once 模式可以省去这种对齐，但恢复后可能产生重复。Unaligned checkpoint 则把在途数据纳入快照，用更多 checkpoint 数据换取反压下更快的 barrier 推进。

### 8.3 Timely/Naiad：Frontier 也不能替代 Checkpoint

Frontier 能证明逻辑时间进度，却不能在进程消失后重建内存中的 arrangement、算子状态和 progress counts。Naiad 为有状态 vertex 和进度跟踪状态另外定义了 `CHECKPOINT/RESTORE` 接口，周期性产生全局一致 checkpoint。Frontier 回答"还能产生什么"，Checkpoint 回答"已经产生和积累的东西怎样恢复"，两者同样正交。

## 9. 用两条边界统一六个问题

到这里可以把文章开头的六个问题重新收拢：

| 问题 | 主要机制 |
| --- | --- |
| 无限数据怎样计算？ | Window、epoch、逻辑时间，把无界输入切成可推进的前缀 |
| 哪些历史数据需要保存？ | 算子代数决定最小状态；窗口、join index、distinct set、arrangement 物化历史影响 |
| 什么时候认为结果完整？ | Watermark、heartbeat、low-watermark、capability/frontier |
| 乱序和迟到怎么办？ | Slack、Watermark、allowed lateness、side output、revision |
| 故障后怎样恢复？ | Ack/replay、consistent checkpoint、source offset、transactional sink |
| 输入变化后怎样更新结果？ | Upsert、retract、changelog、`(data,time,diff)` 差分传播 |

这六个问题最终受两条边界约束：

1. **语义边界**：过去完整到哪里？它由 Watermark 或 Frontier 表达，决定何时输出、何时修订、何时可以忘记历史。
2. **恢复边界**：失败后回到哪里？它由 ack、日志或 Checkpoint 表达，决定输入位置、状态版本与输出提交怎样重新对齐。

一个系统只有 Watermark，没有 Checkpoint，能够按时间触发，却不能可靠恢复；只有 Checkpoint，没有 Watermark，能够从故障中回来，却可能永远不知道窗口何时结束、join 状态何时删除。Revision 则连接两者之外的第三个事实：完成边界有时只是工程承诺，承诺被迟到数据或业务更正打破时，系统必须能够撤回旧答案并传播新答案。

## 10. 本篇小结

时间模型首先是归因模型。Event Time 决定事实属于哪个业务结果，Ingestion Time 描述数据何时进入平台，Processing Time 描述某次执行何时处理它。它们不是三个可以互换的时钟，也不一定是记录上的三个固定字段。

顺序的价值不在于让全世界排成一条队，而在于建立可关闭的计算前缀。Watermark 用标量时间估计 Event-Time 进度；Timely Frontier 在偏序与数据流位置上跟踪仍可能发生的工作。Allowed lateness 决定首次输出之后还愿意为过去保留多久的修订能力。

状态则是历史输入对未来影响的物化：聚合保存 accumulator，join 保存两侧索引，去重保存已见集合，Differential Dataflow 保存可被差分查询和 compact 的 arrangement。时间进度告诉系统何时可以删除这些历史差异。

最后，Watermark 不是容错，Checkpoint 也不是 Event-Time 进度。Storm 的 XOR ack、Flink 的 barrier snapshot、Naiad 的 checkpoint 都在建立恢复边界；Watermark 与 Frontier 建立的是语义边界。把这两条边界分开，才能真正说清流计算为什么既需要时间，也需要存储。

</details>

<blockquote>
后记：这篇文章的最终版已经从头重写——同一份内容，人类改完后是另一篇文章。笑话录的价值在于：它提醒我们，AI 生成的中文里，最危险的不是语法错误，而是那些"听起来完全正确"的错。
</blockquote>
