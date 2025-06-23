from sympy import Function

class WQFunction(Function):
    is_commutative = False

    def __new__(cls, *args, **kwargs):
        # Call SymPy's Function __new__ with args only (no kwargs)
        obj = super().__new__(cls, *args)
        # Store kwargs on the object for later use
        obj._wq_kwargs = kwargs
        return obj

    @property
    def wq_kwargs(self):
        return getattr(self, '_wq_kwargs', {})

    @classmethod
    def eval(cls, *args, **kwargs):
        return None

    def _sympystr(self, printer):
        # Custom string printing to include keyword arguments
        arg_str = ", ".join(printer.doprint(arg) for arg in self.args)
        if self.wq_kwargs:
            kwarg_str = ", ".join(f"{k}={v}" for k, v in self.wq_kwargs.items())
            return f"{self.func.__name__}({arg_str}, {kwarg_str})"
        return f"{self.func.__name__}({arg_str})"


class OperatorRegistry:
    _registry = {}

    @classmethod
    def register(cls):
        def decorator(func):
            cls._registry[func.__name__] = func
            return func
        return decorator

    @classmethod
    def get_operator(cls, name):
        return cls._registry.get(name)

    @classmethod
    def get_all_operator_names(cls):
        return list(cls._registry.keys())

    @classmethod
    def debug_print(cls):
        print("🔍 Registered operators:", cls.get_all_operator_names())

    @classmethod
    def initialize_all(cls):
        from src.alpha_template import operators  # safe, late import
