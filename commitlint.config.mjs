// Config for wagoid/commitlint-github-action, which looks for this exact
// filename by default.
export default {
  extends: ['@commitlint/config-conventional'],
  rules: {
    // Dependabot opens one grouped PR per ecosystem, and its generated body
    // starts with a single "Bumps the dependencies group with N updates in the
    // / directory: [dep](url), [dep](url) and [dep](url)." line that grows past
    // 100 characters as soon as the group holds more than one dependency. The
    // rule cannot be satisfied from our side, so every grouped update PR fails
    // this check. Long URLs in hand-written bodies hit the same wall.
    'body-max-line-length': [0],
  },
}
