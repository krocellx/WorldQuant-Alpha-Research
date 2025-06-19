import hashlib
from sympy import Function, Symbol, symbols, simplify, srepr

from sympy import Function
# from registry import register_wq_function

WQ_FUNCTION_REGISTRY = {}

def register_wq_function(cls):
    WQ_FUNCTION_REGISTRY[cls.__name__] = cls
    return cls

class WQFunction(Function):
    is_commutative = False

    @classmethod
    def eval(cls, *args):
        return None

@register_wq_function
class ts_mean(WQFunction):
    nargs = (2,)

@register_wq_function
class ts_rank(WQFunction):
    nargs = (2,)

@register_wq_function
class ts_zscore(WQFunction):
    nargs = (2,)

@register_wq_function
class zscore(WQFunction):
    nargs = (1,)

@register_wq_function
class rank(WQFunction):
    nargs = (1,)

@register_wq_function
class decay_linear(WQFunction):
    nargs = (2,)

@register_wq_function
class signed_power(WQFunction):
    nargs = (2,)

@register_wq_function
class ts_delta(WQFunction):
    nargs = (2,)

@register_wq_function
class vec_avg(WQFunction):
    nargs = (1,)

@register_wq_function
class vec_sum(WQFunction):
    nargs = (1,)

@register_wq_function
class reverse(WQFunction):
    nargs = (1,)


@register_wq_function
class vector_neut(WQFunction):
    nargs = (2,)

    @classmethod
    def eval(cls, x, y):
        return None  # not symmetric, so order matters


# === Expression Builder ===
class WQExpressionBuilder:
    def __init__(self):
        self._symbol_cache = {}

    def get_symbol(self, name: str):
        if name not in self._symbol_cache:
            self._symbol_cache[name] = Symbol(name)
        return self._symbol_cache[name]

    def parse(self, formula):
        if isinstance(formula, str):
            return self.get_symbol(formula)
        elif isinstance(formula, (int, float)):
            return formula
        elif isinstance(formula, list) and len(formula) > 0:
            op = formula[0]
            args = [self.parse(arg) for arg in formula[1:]]
            op_class = self.get_op_class(op)
            return op_class(*args)
        else:
            raise ValueError(f"Unrecognized formula structure: {formula}")

    def get_op_class(self, op_name: str):
        if op_name not in WQ_FUNCTION_REGISTRY:
            raise ValueError(f"Unknown operator: {op_name}")
        return WQ_FUNCTION_REGISTRY[op_name]

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

import ast
import operator

def parse_wq_formula_string(s: str):
    """
    Parses a WorldQuant-style string formula like 'zscore(ts_delta(x,21))'
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
        else:
            raise ValueError(f"Unsupported node type: {type(node)}")

    tree = ast.parse(s, mode='eval')
    return parse_node(tree.body)


def parse_wq_formula_string_to_key(code):
    """
    Parses a WorldQuant-style string formula and generates a unique key.
    """
    expr = parse_wq_formula_string(code)
    hasher = FormulaHasher()
    return hasher.generate_short_hash(expr)

def parse_wq_formula_string_to_expr(code):
    code = code.replace('"', '')
    nexted_code = parse_wq_formula_string(code)
    builder = WQExpressionBuilder()
    # print(nexted_code)
    expr = builder.parse(nexted_code)
    return expr

import networkx as nx

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

if __name__ == "__main__":
    code = '"zscore(ts_delta(anl4_bvps_mean,21))"'
    code =  code.replace('"', '')
    nexted_code = parse_wq_formula_string(code)
    builder = WQExpressionBuilder()
    expr = builder.parse(nexted_code)
    hasher = FormulaHasher()
    key = hasher.generate_short_hash(expr)
    print(1)