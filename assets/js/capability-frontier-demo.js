(function (host) {
  'use strict';

  // Snapshots of a single time-preserving A -> B channel after progress updates.
  const steps = [
    { cap: 5, message: null, consumed: false, title: 'A 持有 cap@5',
      detail: '当前没有消息。A 以后仍能发送时间 5 的数据。B 还不能确认时间 5 已经收齐。',
      status: '没有消息，不代表不会再来消息。' },
    { cap: 5, message: 'flight', consumed: false, title: 'A 发送 msg@5',
      detail: '消息正在传输。A 仍持有 cap@5，还能继续发送。发送一条消息不会自动释放 capability。',
      status: 'msg@5 正在从 A 前往 B。' },
    { cap: 6, message: 'flight', consumed: false, title: 'A 把 cap 推进到 6',
      detail: 'A 放弃时间 5 的发送权。msg@5 仍未被消费。frontier 仍是 {5}，时间 5 仍未确定。',
      status: 'A 已经不再发送时间 5，旧消息仍在路上。' },
    { cap: 6, message: 'queue', consumed: false, title: 'msg@5 到达 B 的队列',
      detail: '到达队列还不算消费。B 尚未取走这条消息，frontier 仍停在 {5}。',
      status: 'msg@5 已到达输入队列，等待 B 消费。' },
    { cap: 6, message: null, consumed: true, title: 'B 消费最后一条 msg@5',
      detail: '时间 5 的发送权和未消费消息都已消失。frontier 推进到 {6}。B 可以确认时间 5 的输入完整。',
      status: '时间 5 已收齐。A 仍保留时间 6 的发送权。' },
    { cap: null, message: null, consumed: true, title: 'A 释放最后的 cap',
      detail: 'A 不再发送任何数据。通路中也没有未消费消息。frontier 变为空集 ∅，B 的这条输入彻底结束。',
      status: '这条输入不会再收到任何时间的消息。' }
  ];

  function frontier(state) {
    const bounds = [];
    if (state.cap !== null) bounds.push(state.cap);
    if (state.message !== null) bounds.push(5);
    return bounds.length ? [Math.min(...bounds)] : [];
  }
  function complete(state, time) { return !frontier(state).some(f => f <= time); }
  function showSet(values) { return values.length ? '{' + values.join(', ') + '}' : '∅'; }

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { steps, frontier, complete };
  }
  if (!host.document) return;
  const root = host.document.getElementById('capability-frontier-demo');
  if (!root || root.dataset.initialized) return;
  root.dataset.initialized = 'true';
  const find = key => root.querySelector('[data-cf="' + key + '"]');
  const play = find('play');
  const previous = find('previous');
  const next = find('next');
  const token = find('message');
  const reducedMotion = host.matchMedia('(prefers-reduced-motion: reduce)');
  let index = 0;
  let target = 5;
  let timer = null;
  let motion = null;

  const stepList = root.querySelector('.cf-steps');
  const stepButtons = steps.map((step, i) => {
    const li = host.document.createElement('li');
    const button = host.document.createElement('button');
    button.type = 'button';
    button.textContent = String(i + 1);
    button.setAttribute('aria-label', '第 ' + (i + 1) + ' 步：' + step.title);
    button.addEventListener('click', () => { stop(); move(i); });
    li.appendChild(button);
    stepList.appendChild(li);
    return button;
  });
  const times = root.querySelector('.cf-times');
  const timeButtons = [4, 5, 6, 7, 8].map(time => {
    const button = host.document.createElement('button');
    button.type = 'button';
    button.innerHTML = '<strong>' + time + '</strong><span></span>';
    button.addEventListener('click', () => { target = time; renderVerdict(); });
    times.appendChild(button);
    return { time, button };
  });
  root.querySelector('.cf-controls').hidden = false;
  stepList.hidden = false;
  times.hidden = false;

  function renderVerdict() {
    const state = steps[index];
    const f = frontier(state);
    const closed = complete(state, target);
    timeButtons.forEach(({ time, button }) => {
      const done = complete(state, time);
      button.className = done ? 'cf-closed' : 'cf-open';
      button.setAttribute('aria-pressed', String(time === target));
      button.setAttribute('aria-label', '时间 ' + time + '，' + (done ? '已收齐' : '未确定'));
      button.querySelector('span').textContent = done ? '已收齐' : '未确定';
    });
    find('verdict').textContent = '时间 ' + target + (closed ? ' 的输入已经收齐' : ' 还不能确认收齐');
    find('comparison').textContent = f.length === 0
      ? 'frontier = ∅，不存在 f ≤ ' + target + '。'
      : closed
        ? 'frontier 中只有 ' + f[0] + '。' + f[0] + ' > ' + target + '，没有 f ≤ ' + target + '。'
        : 'frontier 中存在 ' + f[0] + ' ≤ ' + target + '，还不能排除后续消息。';
    let reason;
    if (closed) {
      reason = f.length ? '还可能到达的消息从时间 ' + f[0] + ' 开始。' : 'A 已释放发送权。所有已发消息也已消费。';
    } else {
      const causes = [];
      if (state.cap !== null && state.cap <= target) causes.push('A 仍持有 cap@' + state.cap);
      if (state.message !== null && 5 <= target) causes.push('msg@5 还未被消费');
      reason = causes.join('；') + '。';
      if (target > f[0]) reason += 'frontier 只给出下界，不保证这个时间一定有消息。';
    }
    find('reason').textContent = reason;
  }

  function render(oldIndex, animate) {
    const state = steps[index];
    const prior = steps[oldIndex];
    const f = frontier(state);
    find('event-title').textContent = (index + 1) + '. ' + state.title;
    find('event-detail').textContent = state.detail;
    find('counter').textContent = (index + 1) + ' / ' + steps.length;
    find('cap').textContent = state.cap === null ? '已释放' : 'cap@' + state.cap;
    find('cap-note').textContent = state.cap === null ? '不能再发送' : '仍能发送时间 ' + state.cap;
    find('frontier').textContent = showSet(f);
    find('inbox').textContent = state.message === 'queue' ? '队列中有 msg@5' : state.consumed ? 'msg@5 已消费' : '输入队列为空';
    find('message-status').textContent = state.status;
    const capSet = state.cap === null ? '∅' : showSet([state.cap]);
    const msgSet = state.message === null ? '∅' : '{5}';
    find('calculation').textContent = '发送权限 ' + capSet + ' ＋ 未消费消息 ' + msgSet + ' → frontier ' + showSet(f);
    previous.disabled = index === 0;
    next.disabled = index === steps.length - 1;
    stepButtons.forEach((button, i) => {
      if (i === index) button.setAttribute('aria-current', 'step');
      else button.removeAttribute('aria-current');
    });

    if (motion) motion.cancel();
    const position = value => value === 'queue' ? '81%' : '50%';
    token.style.left = position(state.message);
    token.style.opacity = state.message ? '1' : '0';
    if (animate && !reducedMotion.matches && token.animate) {
      if (state.message && prior.message !== state.message) {
        motion = token.animate([
          { left: prior.message ? position(prior.message) : '19%', opacity: 1 },
          { left: position(state.message), opacity: 1 }
        ], { duration: 850, easing: 'ease-in-out' });
      } else if (!state.message && prior.message) {
        motion = token.animate([
          { left: position(prior.message), opacity: 1 },
          { left: position(prior.message), opacity: 0 }
        ], { duration: 450 });
      }
      if (showSet(frontier(prior)) !== showSet(f)) {
        find('frontier').animate([{ transform: 'scale(1)' }, { transform: 'scale(1.12)' }, { transform: 'scale(1)' }], { duration: 450 });
      }
    }
    renderVerdict();
    updatePlayLabel();
  }
  function move(newIndex) {
    const old = index;
    index = Math.max(0, Math.min(steps.length - 1, newIndex));
    if (index === steps.length - 1) stop();
    render(old, true);
  }
  function updatePlayLabel() { play.textContent = timer !== null ? '暂停' : index === steps.length - 1 ? '重新播放' : '播放'; }
  function stop() {
    if (timer !== null) host.clearInterval(timer);
    timer = null;
    updatePlayLabel();
  }
  play.addEventListener('click', () => {
    if (timer !== null) { stop(); return; }
    if (index === steps.length - 1) move(0);
    timer = host.setInterval(() => move(index + 1), 4800);
    updatePlayLabel();
  });
  previous.addEventListener('click', () => { stop(); move(index - 1); });
  next.addEventListener('click', () => { stop(); move(index + 1); });
  host.document.addEventListener('visibilitychange', () => { if (host.document.hidden) stop(); });
  host.addEventListener('pagehide', stop);
  render(0, false);
})(typeof window !== 'undefined' ? window : globalThis);
