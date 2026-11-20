(function (host) {
  'use strict';

  const inputHistory = [
    { time: [0, 0], diff: 1 },
    { time: [0, 1], diff: 1 },
    { time: [1, 0], diff: -1 }
  ];
  const steps = [
    { time: [0, 0], inputCount: 1, label: '直接支持', title: '收到直接支持',
      detail: 'P→甲产生第一份输入。计算时间为 (0,0)。' },
    { time: [0, 1], inputCount: 2, label: '重复支持', title: '收到第二份支持',
      detail: '乙→甲产生第二份输入。两份支持都计入，但目标输出仍然只有一份。' },
    { time: [1, 0], inputCount: 3, label: '较早轮次', title: '删除影响第 0 轮',
      detail: '(0,1) 的支持已保存，但不能计入 (1,0)。本次输入累计为零。' },
    { time: [1, 1], inputCount: 3, label: '较晚轮次', title: '历史触发第 1 轮的修正',
      detail: '(0,1) 的旧支持现在计入。输出历史也计入上一条撤回，因此还需要补发 +1。' }
  ];

  function lessEqual(a, b) { return a[0] <= b[0] && a[1] <= b[1]; }
  function cumulative(history, time) {
    return history.reduce((sum, row) => sum + (lessEqual(row.time, time) ? row.diff : 0), 0);
  }
  function snapshots() {
    const outputHistory = [];
    return steps.map(step => {
      const input = inputHistory.slice(0, step.inputCount);
      const output = outputHistory.slice();
      const inputSum = cumulative(input, step.time);
      const target = inputSum !== 0 ? 1 : 0;
      const outputSum = cumulative(output, step.time);
      const diff = target - outputSum;
      if (diff !== 0) outputHistory.push({ time: step.time, diff });
      return { ...step, input, output, inputSum, target, outputSum, diff };
    });
  }
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { inputHistory, steps, lessEqual, cumulative, snapshots };
  }
  if (!host.document) return;
  const root = host.document.getElementById('recursive-correction-demo');
  if (!root || root.dataset.initialized) return;
  root.dataset.initialized = 'true';
  const find = name => root.querySelector('[data-rc="' + name + '"]');
  const states = snapshots();
  const timeText = time => '(' + time.join(',') + ')';
  const signed = n => n > 0 ? '+' + n : String(n).replace('-', '−');
  let index = 0;

  function renderHistory(target, history, time) {
    target.replaceChildren();
    if (history.length === 0) {
      const row = host.document.createElement('tr');
      const cell = host.document.createElement('td');
      cell.colSpan = 3;
      cell.textContent = '还没有输出';
      row.appendChild(cell);
      target.appendChild(row);
      return;
    }
    history.forEach(item => {
      const included = lessEqual(item.time, time);
      const row = host.document.createElement('tr');
      row.className = included ? 'rc-counted' : 'rc-excluded';
      [timeText(item.time), signed(item.diff), included ? '是' : '否'].forEach(value => {
        const cell = host.document.createElement('td');
        cell.textContent = value;
        row.appendChild(cell);
      });
      target.appendChild(row);
    });
  }
  const stepList = root.querySelector('.rc-steps');
  const buttons = steps.map((step, i) => {
    const button = host.document.createElement('button');
    button.type = 'button';
    button.textContent = (i + 1) + '. ' + step.label;
    button.addEventListener('click', () => render(i));
    stepList.appendChild(button);
    return button;
  });
  function render(nextIndex) {
    index = Math.max(0, Math.min(states.length - 1, nextIndex));
    const state = states[index];
    root.dataset.step = String(index + 1);
    find('heading').textContent = '第 ' + (index + 1) + ' / 4 步：' + state.title + ' · ' + timeText(state.time);
    find('detail').textContent = state.detail;
    renderHistory(find('input'), state.input, state.time);
    renderHistory(find('output'), state.output, state.time);
    find('input-sum').textContent = String(state.inputSum);
    find('output-sum').textContent = String(state.outputSum);
    find('equation').textContent = '目标输出 ' + state.target + ' − 已有输出 ' + state.outputSum + ' = 本次修正 ' + signed(state.diff);
    find('result').textContent = state.diff === 0
      ? '不发送数据更新。第二份输入的影响仍然保留。'
      : '发送甲 @' + timeText(state.time) + ' ' + signed(state.diff) + '，并写入输出历史。';
    buttons.forEach((button, i) => button.setAttribute('aria-pressed', String(i === index)));
    find('previous').disabled = index === 0;
    find('next').disabled = index === states.length - 1;
  }
  find('previous').addEventListener('click', () => render(index - 1));
  find('next').addEventListener('click', () => render(index + 1));
  find('reset').addEventListener('click', () => render(0));
  stepList.hidden = false;
  root.querySelector('.rc-controls').hidden = false;
  render(0);
}(typeof globalThis !== 'undefined' ? globalThis : this));
