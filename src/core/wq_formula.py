import hashlib
from sympy import Function, Symbol, symbols, simplify, srepr
import re
import networkx as nx

from src.alpha_template.operator_registry import WQFunction, OperatorRegistry
OperatorRegistry.initialize_all()
OperatorRegistry.debug_print()

# === Expression Builder ===
class WQExpressionBuilder:
    def __init__(self):
        self._symbol_cache = {}

    def get_symbol(self, name: str):
        if name not in self._symbol_cache:
            self._symbol_cache[name] = Symbol(name)
        return self._symbol_cache[name]

    def parse(self, formula):
        if isinstance(formula, WQFunction):
            return formula
        elif isinstance(formula, Symbol):  # ← Add this
            return formula
        elif isinstance(formula, str):
            return self.get_symbol(formula)

        elif isinstance(formula, (int, float)):
            return formula

        elif isinstance(formula, list) and len(formula) > 0:
            op = formula[0]

            # Manually process args to avoid parsing the last dict (kwargs)
            raw_args = formula[1:]
            kwargs = {}

            if raw_args and isinstance(raw_args[-1], dict):
                kwargs = raw_args.pop()

            args = [self.parse(arg) for arg in raw_args]

            op_class = self.get_op_class(op)
            return op_class(*args, **kwargs)

        else:
            raise ValueError(f"Unrecognized alpha_template structure: {type(formula)} → {formula}")

    def get_op_class(self, op_name: str):
        if op_name not in OperatorRegistry.get_all_operator_names():
            raise ValueError(f"Unknown operator: {op_name}")
        return OperatorRegistry.get_operator(op_name)

# === Key Generator ===
class FormulaHasher:
    @staticmethod
    def generate_key(expr) -> str:
        canonical = simplify(expr)
        string_repr = srepr(canonical)
        return hashlib.sha256(string_repr.encode()).hexdigest()

    @staticmethod
    def generate_short_hash(expr, length=12):
        canonical_repr = srepr(simplify(expr))
        full_hash = hashlib.sha256(canonical_repr.encode()).hexdigest()
        return full_hash[:length]

    @staticmethod
    def generate_short_hash_with_settings(expr, settings_str, length=12):
        """
        Generate a short hash combining the canonical formula and the settings.
        """
        canonical_repr = srepr(simplify(expr))
        combined = f"{canonical_repr}|{settings_str}"
        full_hash = hashlib.sha256(combined.encode()).hexdigest()
        return full_hash[:length]

import ast
import operator

def parse_wq_formula_string(s: str):
    """
    Parses a WorldQuant-style string alpha_template like 'zscore(ts_delta(x,21))'
    into a nested list format: ['zscore', ['ts_delta', 'x', 21]]
    """
    unary_operators = {
        ast.USub: operator.neg,
        ast.UAdd: operator.pos
    }
    def parse_node(node):
        if isinstance(node, ast.Call):
            func_name = node.func.id
            args = [parse_node(arg) for arg in node.args]
            kwargs = {
                kw.arg: parse_node(kw.value) for kw in node.keywords
            }
            if kwargs:
                # Convert keyword args to a dict at the end (optional)
                return [func_name] + args + [kwargs]
            else:
                return [func_name] + args
        elif isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Constant):  # Python 3.8+
            return node.value
        elif isinstance(node, ast.Num):  # Python <3.8 fallback
            return node.n
        elif isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.USub):  # e.g., -x
                return ['reverse', parse_node(node.operand)]
            elif isinstance(node.op, ast.UAdd):  # e.g., +x
                return parse_node(node.operand)
            else:
                raise ValueError(f"Unsupported unary op: {type(node.op)}")
        elif isinstance(node, ast.Compare):
            # Only support single comparison for now
            left = parse_node(node.left)
            right = parse_node(node.comparators[0])
            op = type(node.ops[0])
            op_map = {
                ast.Gt: 'gt',
                ast.Lt: 'lt',
                ast.GtE: 'gte',
                ast.LtE: 'lte',
                ast.Eq: 'eq',
                ast.NotEq: 'neq'
            }
            op_name = op_map[op]
            return [op_name, left, right]

        elif isinstance(node, ast.BoolOp):
            op_type = type(node.op)
            op_name = {ast.And: 'and', ast.Or: 'or'}.get(op_type)
            values = [parse_node(v) for v in node.values]
            return [op_name] + values

        elif isinstance(node, ast.BinOp):
            op_type = type(node.op)
            op_name = {
                ast.Add: 'add',
                ast.Sub: 'subtract',
                ast.Mult: 'multiply',
                ast.Div: 'divide'
            }.get(op_type)
            return [op_name, parse_node(node.left), parse_node(node.right)]
        else:
            raise ValueError(f"Unsupported node type: {type(node)}")

    tree = ast.parse(s, mode='eval')
    return parse_node(tree.body)


def parse_wq_formula_string_to_key(str_expr: str, settings_str: str) -> str:
    """
    Parses a WorldQuant-style formula and associated settings to generate a unique key.
    """
    hasher = FormulaHasher()
    return hasher.generate_short_hash_with_settings(str_expr, settings_str)

def parse_wq_formula_string_to_expr(code: str):
    code = code[1:-1] if code.startswith('"') and code.endswith('"') else code
    code = code.strip()
    print(code)
    if "\n" in code or any("=" in line for line in code.splitlines()):
        # Treat as block with possible assignments
        expr, _ = parse_formula_block(code)
        print(expr)
    else:
        # Treat as single-line formula

        nested_expr = parse_wq_formula_string(code)
        builder = WQExpressionBuilder()
        expr = builder.parse(nested_expr)
        print(expr)
    return expr

def parse_formula_block(code: str):
    """
    Parses a multi-line formula block with intermediate variables and trade_when.
    Returns the final formula expression (SymPy), a symbol table of assignments, and parsed tree.
    """
    lines = code.splitlines()
    symbol_table = {}
    final_expr = None  # fallback if no trade_when

    for raw_line in lines:
        line = raw_line.strip()

        # Remove inline comments
        if '#' in line:
            line = line.split('#')[0].strip()
        if not line:
            continue

        line = line.rstrip(';')  # <- 🔥 THIS IS THE FIX

        if '=' in line and not line.startswith("trade_when"):
            # Assignment
            name, expr_str = [x.strip() for x in line.split('=', 1)]
            expr_str = expr_str.rstrip(';')  # <- Optional: extra safety
            nested_expr = parse_wq_formula_string(expr_str)
            symbol_table[name] = nested_expr

        elif line.startswith("trade_when"):
            # Final line
            line = line.rstrip(';')  # <- 🔥 THIS IS THE FIX
            final_expr = parse_wq_formula_string(line)

    if final_expr is None:
        if symbol_table:
            last_key = list(symbol_table)[-1]
            final_expr = symbol_table[last_key]
        else:
            # fallback: check if the last non-comment line is a formula
            for line in reversed(lines):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                try:
                    line = line.rstrip(';')
                    final_expr = parse_wq_formula_string(line)
                    break
                except Exception:
                    continue
            if final_expr is None:
                raise ValueError(f"No valid formula found. Parsed lines: {lines}")

    # Substitute variables recursively
    builder = WQExpressionBuilder()
    resolved_expr = substitute_symbols(final_expr, symbol_table, builder)
    return resolved_expr, symbol_table


def substitute_symbols(nested_expr, symbol_table, builder):
    if isinstance(nested_expr, str):
        return builder.parse(symbol_table.get(nested_expr, nested_expr))

    elif isinstance(nested_expr, (int, float)):
        return builder.parse(nested_expr)

    elif isinstance(nested_expr, dict):
        # Recursively substitute symbols inside the dict
        return {
            k: substitute_symbols(v, symbol_table, builder)
            for k, v in nested_expr.items()
        }

    elif isinstance(nested_expr, list):
        args = []
        for arg in nested_expr[1:]:
            # If last arg is dict → pass directly without parsing
            if isinstance(arg, dict):
                args.append(arg)
            else:
                args.append(substitute_symbols(arg, symbol_table, builder))

        return builder.parse([nested_expr[0]] + args)

    else:
        raise ValueError(f"Unsupported nested expression structure: {type(nested_expr)}")

class FormulaDAG:
    def __init__(self):
        self.graph = nx.DiGraph()
        self.expr_lookup = {}  # optional: to store exprs by key

    def add_formula(self, expr):
        key = FormulaHasher.generate_key(expr)
        if key not in self.graph:
            self.graph.add_node(key, expr=expr)
            self.expr_lookup[key] = expr

        for arg in expr.args:
            if isinstance(arg, WQFunction):
                subkey = FormulaHasher.generate_key(arg)
                self.add_formula(arg)  # recursively add subformula
                self.graph.add_edge(subkey, key)  # subexpr → full expr

    def add_all_formulas(self, expr_list):
        for expr in expr_list:
            self.add_formula(expr)

    def assign_tiers(self):
        tiers = {}
        for node in nx.topological_sort(self.graph):
            preds = list(self.graph.predecessors(node))
            if not preds:
                tiers[node] = 0
            else:
                tiers[node] = 1 + max(tiers[p] for p in preds)
        return tiers

def make_tracker_key(row):
    formula = row['parsed_formula']
    settings = '|'.join([
        str(row['neutralization']),
        str(row['decay']),
        str(row['truncation']),
        str(row['delay']),
        str(row['universe']),
        str(row['region']),
        str(row['pasteurization']),
        str(row['nanHandling']),
    ])
    return parse_wq_formula_string_to_key(formula, settings)

from sympy import Basic, Symbol
from sympy.core.numbers import Integer, Float
from collections import defaultdict

def sympy_expr_to_tiers_with_custom_logic(expr):
    if isinstance(expr, str):
        raise ValueError("Expected parsed SymPy expression, got string.")

    tiers = defaultdict(list)

    def walk(e, parent_func=None):
        if isinstance(e, Symbol):
            # Exclude symbols used as grouping keys in certain functions
            if parent_func not in {"group_neutralize", "bucket"}:
                tiers[0].append(str(e))
                return 0
            return -1

        elif isinstance(e, (Integer, Float, int, float)):
            return -1

        elif hasattr(e, 'func') and hasattr(e, 'args'):
            args = list(e.args)
            subtiers = [walk(arg, str(e.func)) for arg in args]
            current_tier = max([t for t in subtiers if t >= 0], default=-1) + 1

            expr_str = f"{str(e.func)}({', '.join(map(str, e.args))})"

            # Custom logic for vector ops
            if str(e.func) == "reverse":
                arg0_func = getattr(args[0], 'func', None)
                if str(arg0_func) in {"vec_avg", "vec_sum"}:
                    current_tier = 0
                    tiers[0].append(expr_str)
                    return current_tier

            elif str(e.func) in {"vec_avg", "vec_sum"}:
                current_tier = 0
                tiers[0].append(expr_str)
                return current_tier

            tiers[current_tier].append(expr_str)
            return current_tier

        else:
            raise ValueError(f"Unsupported node: {type(e)} → {e}")

    walk(expr)
    return dict(tiers)


if __name__ == "__main__":
    code = """when = pcr_oi_180 < 1;

iv_difference = implied_volatility_call_180 - implied_volatility_put_180;

iv_difference_n = ts_decay_linear(iv_difference, 21);

# std_group =densify(bucket(rank(historical_volatility_150) * rank(volume * close), range=""0.0, 1, 0.05""));
# std_group = densify(bucket(rank(pcr_oi_180) * rank(ts_decay_linear(volume * close, 21)), range=""0.0,1.0,0.05""));
std_group = densify(bucket(rank(pcr_oi_180) * ts_decay_linear(rank(volume * close), 21), range=""0.0,1.0,0.05""));
# std_group = densify(bucket(rank(pcr_oi_180) * rank(volume * close), range=""0.0,1.0,0.05""));

trade_when(when, group_neutralize(scale(iv_difference_n, scale=1), std_group), -1)
"""
    parse_wq_formula_string_to_key(code)