# qingzhu521.github.io · 流计算讲义系列 Design System

范围说明：这是一个 Jekyll + minima（2.5.2，无 dark skin 支持，实际渲染为浅色主题：背景 `#fdfdfd`、正文 `#111`、内容列宽 800px）的个人技术博客。本系统只约束**文章正文内嵌的图形、表格、交互组件**的视觉语言——不修改 `_config.yml` / `main.scss` / 主题布局。所有 token 通过文章内 `<style>` 块以 `.post-content` 为作用域生效，不泄漏到站点其他页面。

## 0. Research Log（greenfield）

- Embedded refs：在 `references/design/_INDEX.md` 定位候选后，从 Layer B 里短选 `notion`、`linear.app`、`stripe` 三个偏编辑感/低饱和度方向；因为本文是**长文技术讲义**而非产品落地页，选定 **Layer A = `minimalist-skill.md`**（warm monochrome、editorial、flat bento、禁用重阴影/渐变）+ **Layer B = `notion.md`**（warm neutral 纸感、whisper border、单一饱和强调色、大量留白），完整通读两份文件，理由：读者是"读过美团/字节技术博客"的工程师，需要的是安静、可信、耐看的文档感，而不是营销落地页的视觉冲击。
- Lazyweb / Imagen：按"研究车道可与 Jekyll 博文规模成比例"跳过——本任务没有生成位图插图的诉求（全部图形是内联 SVG），也没有需要真实产品截图做基准的界面；跳过理由：**范围不适用**，不是工具/网络不可用。
- 站点实测：拉取 `minima` 2.5.2 源码与本地 `bundle exec jekyll build` 产物，确认站点当前有效渲染是**浅色**（`minima: skin: dark` 在 2.5.2 里是无效配置项，该版本尚未实现 skin 切换）。这决定了本系统必须是浅色纸感体系，而不是深色终端风——与 notion.md 的取向天然吻合。
- Skipped：品牌视觉扫描（`open-design` 兜底库）——curated 集合里的 `notion` 已完全够用，未触发兜底条件。

## 1. Atmosphere & Identity

一页安静的纸。内容是主角，图形是注解，不是装饰。签名手法是**克制的暖色分区**：正文用米白纸背景上的近黑文字，图形卡片退到更暖的奶油色纸面上，用一条极细的暖灰描边把"图"和"文"分开，而不是用阴影把图形抬高出页面。色彩只服务语义——teal 和赭橙分别绑定"两种调度谱系"的身份，红色只允许出现在"重复/浪费"这一件事上，紫色只允许出现在"最终聚合结果"这一件事上。读者应该能在扫一眼配色时就知道自己在看哪条主线，而不需要读图注。

## 2. Color

### Palette

| Role | Token | Value | Usage |
|---|---|---|---|
| Surface/page | `--surface-page` | `#fdfdfd`（minima 默认，不覆盖） | 页面背景，继承主题 |
| Surface/paper | `--surface-paper` | `#fffdf8` | 图形卡片、交互面板背景 |
| Surface/paper-sunken | `--surface-sunken` | `#f6f1e7` | 面板内的次级分区（如状态面板底色） |
| Text/primary | `--ink-900` | `#1c1917` | 图内文字、卡片标题 |
| Text/secondary | `--ink-600` | `#57534e` | 图注、叙述文字、次级说明 |
| Text/muted | `--ink-400` | `#a8a29e` | 占位/未激活状态文字 |
| Border/default | `--border-default` | `#e8e0d4` | 卡片描边、分隔线 |
| Border/subtle | `--border-subtle` | `#efe8db` | 面板内部分隔 |
| Line/inactive | `--edge-idle` | `#d6d3d1` | 图中未激活的边、未发现的节点描边 |
| Accent/teal（Timely·异步谱系） | `--teal-ink` / `--teal-bg` | `#0f766e` / `#ccfbf1` | Timely 相关一切：节点高亮、标题色、进度线 |
| Accent/orange（BSP·同步谱系） | `--orange-ink` / `--orange-bg` | `#9a3412` / `#ffedd5` | BSP/superstep 相关一切：节点高亮、标题色、屏障线 |
| Accent/red（浪费·重复） | `--red-ink` / `--red-bg` | `#b91c1c` / `#fee2e2` | 只用于"重复消息 / 作废 / 白做的功"这一条语义，虚线 |
| Accent/purple（聚合结果） | `--purple-ink` / `--purple-bg` | `#6d28d9` / `#ede9fe` | 只用于"最终聚合输出"（GROUP BY / SUM 汇总框） |
| Link（继承 minima） | `--link` | `#2a7ae2` | 不覆盖，保持站点一致的链接色 |

### Rules

- 颜色是稀缺资源：teal 和 orange 是两条**谱系色**，一旦某个图形属于 BSP 讨论就全篇用 orange，属于 Timely 就全篇用 teal，禁止在同一张图里把它们当装饰混用。
- 这两条谱系色在 §5 之前已经埋了一次伏笔：§1 里"可以干净并行、不需要等待"的一侧（SUM/distributive）用 teal，"必须全部数据碰面、有阻塞"的一侧（MEDIAN/holistic）用 orange。这不是复用撞色，是同一条隐喻贯穿全篇——teal 始终代表"无阻塞"，orange 始终代表"要等/要栅栏"，到 §5 具体化成 Timely 与 BSP 只是这条隐喻第一次有名字。
- 红色虚线严格只表示"重复/被作废的消息"；紫色严格只表示"聚合之后的最终结果"。这两种颜色不出现在任何其他语境，否则读者会把语义搞混。
- 未激活/未发现的节点统一用白底 + `--edge-idle` 描边，不用灰底——灰底会和"已发现但不属于当前谱系"产生歧义。
- 不引入本表之外的颜色。新增语义先扩表。

## 3. Typography

### 覆盖范围

`.post-content` 内联覆盖 minima 默认字体设置（原栈含 Roboto，中文渲染下字重不稳定），仅作用于本文，不影响站点其他页面。

### Font Stack

- 正文/标题（不分离 serif/sans——中文语境下 Latin 编辑体 serif 在无网络字体兜底时渲染不稳定，故正文与标题统一用系统 sans，靠字重和字号建立层级）：
  `-apple-system, BlinkMacSystemFont, "PingFang SC", "Helvetica Neue", "Segoe UI", "Microsoft YaHei", sans-serif`
- 代码/公式/时间戳：`ui-monospace, "SF Mono", Menlo, Consolas, "Cascadia Code", monospace`

### Scale

| Level | Size | Weight | Line Height | Tracking | Usage |
|---|---|---|---|---|---|
| Display（H1，minima 已渲染） | 继承 minima `.post-title`（不覆盖） | — | — | — | 文章标题 |
| H2 | 1.5rem / 24px | 700 | 1.35 | -0.01em | 一级章节（1. 2. 3. …） |
| H3 | 1.2rem / 19.2px | 600 | 1.4 | 0 | 子章节（3.1 / 5.2 …） |
| Body | 1.0625rem / 17px | 400 | 1.85 | 0 | 正文段落（中文加大行高） |
| Body/table | 0.9375rem / 15px | 400 | 1.6 | 0 | 表格单元格 |
| Caption | 0.85rem / 13.6px | 500 | 1.5 | 0.01em | 图注、归因说明 |
| Label（图内） | 12px | 600 | 1.2 | 0 | SVG 节点/标签文字 |
| Micro（步骤序号/时间戳） | 11px | 700 | 1.2 | 0.02em | superstep / iteration 标记 |

### Rules

- 中文正文行高 1.85——比拉丁文常见的 1.6 更松，避免 CJK 字符视觉拥挤。
- 标题不用 serif：这是对 `minimalist-skill.md` 默认建议（editorial serif 大标题）的**明确偏离**，理由已写在字体栈说明里，避免读者在无法离线加载字体的浏览器上看到系统兜底 serif（如 Windows 上的 SimSun）破坏整体质感。
- 表格、图注字号不小于 13px。

## 4. Spacing & Layout

### Base Unit：4px，复用 minimalist-skill 的松量原则，但收紧到适配 800px 窄列的技术长文

| Token | Value | Usage |
|---|---|---|
| `--space-1` | 4px | 图内节点与标签间距 |
| `--space-2` | 8px | 徽章内边距、表格单元格内边距 |
| `--space-3` | 12px | 控件按钮内边距 |
| `--space-4` | 16px | 卡片内边距（默认） |
| `--space-6` | 24px | 卡片内边距（宽图/交互面板） |
| `--space-8` | 32px | 图形卡片与上下文的垂直间距 |

### Grid

- 内容列宽由 minima 决定：800px 容器，减去左右 30px padding，有效 740px（≤800px 视口时再减）。所有图形卡片 `width:100%`，SVG 用 `viewBox` + `max-width:100%; height:auto`，在 375px 视口等比缩小，不产生横向滚动。
- 表格用 `display:block; overflow-x:auto` 而不是额外包一层 `<div>`——两个原因：(1) kramdown 对"HTML 块内嵌 Markdown 表格"要求空行规则严格，多一层包裹容易在渲染时把表格降级成代码块；(2) 直接在 `table` 选择器上加 `display:block;overflow-x:auto` 是等价效果且更稳。这条记在 §8 Accepted Debt。

## 5. Components

### Figure Card `.fig-card`
- **结构**：`<figure class="fig-card"><svg viewBox="..." class="fig-svg">…</svg><figcaption class="fig-caption">…</figcaption></figure>`
- **样式**：背景 `--surface-paper`，`1px solid --border-default`，`border-radius:14px`，`padding: var(--space-6)`，`margin: var(--space-8) 0`。
- **状态**：静态图形无交互态；仅一种呈现。
- **可访问性**：SVG 根节点加 `role="img"` + `aria-label`，文字用真实 `<text>` 而非图片位图，可被屏幕阅读器/复制选中。
- **Layout**：block-level，独立占一行，不与正文混排。

### Node-Link Diagram（SVG 内部约定，跨所有图形复用）
- 节点：`circle r=15~16`，未发现 `fill:#fff stroke:var(--edge-idle)`；按所属谱系着色 `fill:var(--teal-bg|--orange-bg) stroke:var(--teal-ink|--orange-ink)`。
- 边：默认 `stroke:var(--edge-idle) stroke-width:1.6`；本轮激活 `stroke:谱系色 stroke-width:2.6`；重复/作废 `stroke:var(--red-ink) stroke-dasharray:5 4`。
- 标签：`font-size:12px fill:var(--ink-900)`，比例标签（如 70%）用 `font-size:10px fill:var(--ink-400)`。
- 箭头：统一一个 `<marker>` 定义每个 SVG 各自命名空间化（`id` 加 svg 前缀避免多图冲突）。

### Concept Table（GFM 管道表格 + `.post-content table` 全局样式，非独立组件）
- **样式**：`border-collapse:collapse; width:100%`，表头 `background:var(--surface-sunken); font-weight:600`，行分隔 `border-bottom:1px solid var(--border-subtle)`，单元格 `padding:10px 12px`。
- **强调列**（如"本轮白做的功"）：正文里用 `**粗体**` 标出即可，不额外加色块——保持表格的信息密度和可扫描性。
- **状态**：无交互态，静态表格。
- **响应式**：见 §4 Grid 的 `display:block;overflow-x:auto` 规则，375px 下可横向滚动而不撑破布局。

### Callout `.callout--insight` / `.callout--caution`
- **结构**：`<div class="callout callout--insight"><p>…</p></div>`
- **样式**：左侧 4px 色条 + 背景 `--surface-sunken`，`padding: var(--space-4) var(--space-6)`，`border-radius:0 10px 10px 0`。`--insight` 用 `--teal-ink` 色条（用于"归因"性质的收束句——回答"这一节解决了什么问题"）；`--caution` 用 `--orange-ink` 色条（用于必须澄清的易误读之处，如"重复消息代价不为零"）。
- **使用节制**：全文每节最多一个 callout，不用它替代正文叙述，只用于确实需要视觉分隔的收束句。

### Term Tag（术语首次出现的行内标记）`.term`
- **结构**：`<span class="term">Δ 集合</span>`
- **样式**：`border-bottom:1px dashed var(--ink-400); font-weight:600`，无背景色、无 tooltip——纯排版手法，帮助读者用眼睛扫描定位关键术语（BSP 屏障 / Δ 集合 / frontier 等），不引入交互。

### Step Lab `.step-lab`（本文唯一交互组件：BSP vs Timely 步进动画）
- **结构**：外层卡片 → 两栏 `.step-lab__col`（BSP / Timely），每栏含 `<svg>` 节点图 + `.step-lab__badge`（当前 superstep/事件序号）+ `.step-lab__note`（本步叙述）+ `.step-lab__known`（已知集合状态面板，芯片列表）；底部单条控件栏 `.step-lab__controls`（上一步/下一步/自动播放/重置），两栏共享同一步进游标。
- **Variants**：无——只有一种布局，左 BSP（orange 谱系）、右 Timely（teal 谱系），移动端 `flex-wrap` 纵向堆叠。
- **Spacing**：外卡 `--space-6`，两栏 `gap: var(--space-4)`，状态面板芯片 `gap: var(--space-2)`。
- **States**：
  - 控件按钮：default（白底 + `--border-default` 描边）/ hover（背景转 `--surface-sunken`）/ active（`transform:scale(0.97)`）/ focus-visible（`2px solid` outline，颜色取对应谱系强调色）/ disabled（游标到边界时上一步或下一步置灰，`opacity:.4; cursor:not-allowed`）。
  - 自动播放按钮有明确的播放/暂停两态文案切换，不用图标暗示（避免图标语义歧义）。
  - 节点/边：见"Node-Link Diagram"状态定义。
  - 已知集合芯片：新加入芯片 `background:谱系色bg`，未加入 `background:transparent;border:1px dashed var(--edge-idle);color:var(--ink-400)`。
- **Accessibility**：按钮为真实 `<button>`，键盘可达；`.step-lab__note` 用 `aria-live="polite"`，步进时朗读当前叙述；`prefers-reduced-motion` 时把颜色/描边过渡时长压到 0ms（步进逻辑本身不受影响，只是去掉过渡动画）。
- **Motion**：节点 `fill/stroke` 过渡 `180ms ease-out`；边 `stroke/stroke-width` 过渡 `180ms ease-out`；叙述文字用 `opacity` 从 0→1 淡入 `150ms`（不做位移，避免布局抖动）。
- **Layout**：两列 `flex` 布局，`min-width:260px`，容器为独立 primitive，不与相邻正文共享滚动上下文。

## 6. Motion & Interaction

### Timing

| Type | Duration | Easing | Usage |
|---|---|---|---|
| Micro | 150-180ms | ease-out | Step Lab 节点/边状态切换、按钮 active |
| Standard | 200ms | ease-in-out | Callout/Term 的 hover 反馈（如有） |
| Reveal | 150ms | linear | 叙述文字淡入 |

### Rules

- 只动 `fill`、`stroke`、`stroke-width`、`opacity`、`transform`，不动布局属性。
- Step Lab 的"自动播放"是唯一带自身节奏的动画，节奏 1.4s/步，到最后一步自动停止（不循环，循环播放会让读者误以为在演示无限过程）。
- `prefers-reduced-motion: reduce` 时所有过渡时长归零，但步进控制、自动播放的**功能**保留（这是内容的一部分，不是装饰性动效）。

## 7. Depth & Surface

### Strategy：borders-only

不使用阴影或色调分层来表达层次，只用 `1px solid var(--border-default)` 描边把图形卡片、交互面板从正文中分离出来——与 notion.md 的 whisper-border 哲学一致，也是 minimalist-skill 的强制约束（禁用 Tailwind 默认重阴影）。

| Type | Value | Usage |
|---|---|---|
| Default | `1px solid var(--border-default)` | 图形卡片、交互面板外框 |
| Subtle | `1px solid var(--border-subtle)` | 面板内部分区（如状态面板与叙述区之间） |

## 8. Accessibility Constraints & Accepted Debt

### Constraints

- WCAG 目标 AA：正文对比度 `#1c1917` on `#fffdf8` ≈ 16:1，远超 4.5:1 下限；`--orange-ink`(#9a3412) on `--orange-bg`(#ffedd5) ≈ 6.8:1，`--teal-ink`(#0f766e) on `--teal-bg`(#ccfbf1) ≈ 5.1:1，均达标。
- Step Lab 所有控件键盘可达、有可见 focus 环；`aria-live` 播报当前叙述；SVG 图形提供 `role="img"` + `aria-label` 兜底文字描述。
- `prefers-reduced-motion` 生效时保留功能、去掉过渡动画（见 §6）。

### Accepted Debt

| Item | Location | Why accepted | Owner / Exit |
|---|---|---|---|
| 正文标题不使用 editorial serif（偏离 minimalist-skill 默认建议） | 全文 H1-H3 | 离线站点无法加载网络衬线字体，中文语境下系统兜底 serif（如 SimSun）会破坏质感；系统 sans 在所有平台渲染更稳定 | 若未来引入本地打包字体文件（不依赖 CDN），可重新评估 |
| 表格响应式用 `display:block;overflow-x:auto` 而非包裹 `<div>` | `.post-content table` | kramdown 对 HTML 块内嵌 Markdown 表格的空行规则脆弱，直接选择器更稳妥 | 若表格列数继续增加导致横向滚动体验变差，改为按屏宽拆分为两张竖排小表 |
| 未做深色模式 | 全文 | `_config.yml` 声明的 `skin: dark` 在当前 minima 2.5.2 版本下不生效，站点实际是浅色主题，暗色适配没有可验证的渲染目标 | minima 升级到支持 dark skin 的版本后补齐 |
