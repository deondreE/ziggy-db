// Ziggy Query Language (ZQL)
// Tree‑sitter grammar — Fixed v0.1
// Resolves conflict between select_item, unary_expression, expression.

module.exports = grammar({
  name: 'zql',

  extras: ($) => [/\s+/, $.comment],
  word: ($) => $.identifier,

  // declare conflict so Tree‑sitter knows SELECT context can overlap expr
  conflicts: ($) => [[$.unary_expression], [$.from_source]],

  rules: {
    query: ($) => seq(repeat($.statement), optional(';')),

    statement: ($) =>
      choice(
        $.select_stmt,
        $.alter_stmt,
        $.show_stmt,
        $.diff_stmt,
        $.rollback_stmt,
        $.explain_stmt,
        $.create_func_stmt,
      ),

    /* ---------------- SELECT ---------------- */

    select_stmt: ($) =>
      seq(
        'SELECT',
        $.select_list,
        optional($.from_clause),
        optional($.where_clause),
        optional($.temporal_clause),
        optional($.group_clause),
        optional($.order_clause),
        optional($.limit_clause),
      ),

    select_list: ($) => choice('*', commaSep1($.select_item)),

    // Give select_item high precedence
    select_item: ($) =>
      prec.right(10, seq($.expression, optional(seq('AS', $.identifier)))),

    from_clause: ($) => seq('FROM', $.from_source, repeat($.join_clause)),

    from_source: ($) =>
      choice(
        seq($.identifier, optional(seq('AS', $.identifier))),
        seq('hot', '(', $.identifier, ')'),
        seq('cold', '(', $.identifier, ')'),
      ),

    join_clause: ($) =>
      seq(optional($.join_type), 'JOIN', $.from_source, 'ON', $.expression),

    join_type: (_) => choice('INNER', 'LEFT', 'RIGHT', 'FULL'),

    where_clause: ($) => seq('WHERE', $.expression),

    group_clause: ($) => seq('GROUP', 'BY', commaSep1($.expression)),

    order_clause: ($) =>
      seq(
        'ORDER',
        'BY',
        commaSep1(seq($.expression, optional(choice('ASC', 'DESC')))),
      ),

    limit_clause: ($) => seq('LIMIT', $.number),

    temporal_clause: ($) =>
      choice(
        seq('AT', 'VERSION', $.number),
        seq('AS', 'OF', $.time_expr),
        seq('BETWEEN', $.time_expr, 'AND', $.time_expr),
      ),

    time_expr: ($) => choice($.string, $.timestamp),

    /* ---------------- ALTER / SCHEMA MGMT ---------------- */

    alter_stmt: ($) =>
      seq('ALTER', 'TABLE', $.identifier, commaSep1($.alter_action)),

    alter_action: ($) =>
      choice(
        seq('ADD', 'COLUMN', $.column_def),
        seq('DROP', 'COLUMN', $.identifier),
        seq('COERCE', $.identifier, 'TO', $.type_name),
      ),

    column_def: ($) =>
      seq($.identifier, $.type_name, optional(seq('DEFAULT', $.literal))),

    show_stmt: ($) =>
      choice(
        seq('SHOW', 'SCHEMA', optional(seq('FOR', $.identifier))),
        seq('SHOW', 'VERSION', optional(seq('TREE', 'FOR', $.identifier))),
        seq('SHOW', 'STORAGE', 'MAP', 'FOR', $.identifier),
      ),

    diff_stmt: ($) =>
      seq('DIFF', 'SCHEMA', $.identifier, 'VERSION', $.number, 'TO', $.number),

    rollback_stmt: ($) =>
      seq('ROLLBACK', 'SCHEMA', $.identifier, 'TO', 'VERSION', $.number),

    explain_stmt: ($) => seq('EXPLAIN', 'PLAN', 'FOR', $.select_stmt),

    /* ---------------- FUNCTION DEF ---------------- */

    create_func_stmt: ($) =>
      seq(
        'CREATE',
        'FUNCTION',
        $.identifier,
        '(',
        optional(commaSep($.param)),
        ')',
        'RETURNS',
        $.type_name,
        'AS',
        $.code_block,
      ),

    param: ($) => seq($.identifier, optional($.type_name)),

    code_block: () => token(seq('{', /[^}]*/, '}')),

    /* ---------------- EXPRESSIONS ---------------- */

    // Set explicit lower precedence on unary to avoid conflict
    unary_expression: ($) =>
      prec.right(1, seq(optional(choice('+', '-', 'NOT')), $.expression)),

    expression: ($) =>
      prec.right(
        choice(
          $.binary_expression,
          $.unary_expression,
          $.function_call,
          $.member_access,
          $.optional_access,
          $.identifier,
          $.literal,
          seq('(', $.expression, ')'),
        ),
      ),

    binary_expression: ($) =>
      prec.left(seq($.expression, $.binary_operator, $.expression)),

    binary_operator: () =>
      choice(
        '+',
        '-',
        '*',
        '/',
        '=',
        '!=',
        '<',
        '>',
        '<=',
        '>=',
        'LIKE',
        'IN',
        'AND',
        'OR',
      ),

    member_access: ($) => seq($.identifier, '.', $.identifier),

    optional_access: ($) => seq($.identifier, '?.', $.identifier),

    function_call: ($) =>
      seq($.identifier, '(', optional(commaSep($.expression)), ')'),

    /* ---------------- LITERALS / TYPES / MISC ---------------- */

    literal: ($) => choice($.number, $.string, $.timestamp, $.boolean, 'NULL'),

    number: () => /\d+(\.\d+)?/,
    string: () => /'(?:[^']*)'/,
    timestamp: () => token(seq('TIMESTAMP', "'", /[^']*/, "'")),
    boolean: () => choice('TRUE', 'FALSE'),

    type_name: () =>
      choice('INT', 'FLOAT', 'TEXT', 'BOOLEAN', 'TIMESTAMP', 'JSON', 'VARIANT'),

    identifier: () => /[A-Za-z_][A-Za-z0-9_]*/,

    comment: () => token(choice(/--.*/, /\/\*[\s\S]*?\*\//)),
  },
});

/* ---------------- Helpers ---------------- */
function commaSep(rule) {
  return optional(seq(rule, repeat(seq(',', rule))));
}
function commaSep1(rule) {
  return seq(rule, repeat(seq(',', rule)));
}
