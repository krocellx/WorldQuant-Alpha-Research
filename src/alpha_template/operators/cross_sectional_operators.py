from src.alpha_template.operator_registry import OperatorRegistry, WQFunction

@OperatorRegistry.register()
class zscore(WQFunction):
    nargs = (1,)

@OperatorRegistry.register()
class rank(WQFunction):
    nargs = (1,)

@OperatorRegistry.register()
class scale(WQFunction):
    nargs = None  # allows variable-length args

    @classmethod
    def eval(cls, *args):
        # Optionally normalize or check arguments
        return None

@OperatorRegistry.register()
class vector_neut(WQFunction):
    nargs = (2,)

    @classmethod
    def eval(cls, x, y):
        return None  # not symmetric, so order matters

@OperatorRegistry.register()
class normalize(WQFunction):
    nargs = None  # allows variable-length args

    @classmethod
    def eval(cls, *args):
        # Optionally normalize or check arguments
        return None

@OperatorRegistry.register()
class quantile(WQFunction):
    nargs = None  # allows variable-length args

    @classmethod
    def eval(cls, *args):
        # Optionally normalize or check arguments
        return None
