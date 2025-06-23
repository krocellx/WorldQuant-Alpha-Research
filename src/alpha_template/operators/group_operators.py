from src.alpha_template.operator_registry import OperatorRegistry, WQFunction

# Logical
@OperatorRegistry.register()
class group_neutralize(WQFunction):
    nargs = (2,)