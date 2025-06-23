from src.alpha_template.operator_registry import OperatorRegistry, WQFunction

# Logical
@OperatorRegistry.register()
class lt(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class lte(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class gt(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class gte(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class eq(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class neq(WQFunction):
    nargs = (2,)
