from src.alpha_template.operator_registry import OperatorRegistry, WQFunction

@OperatorRegistry.register()
class vec_avg(WQFunction):
    nargs = (1,)

@OperatorRegistry.register()
class vec_sum(WQFunction):
    nargs = (1,)
