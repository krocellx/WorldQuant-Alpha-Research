from src.alpha_template.operator_registry import OperatorRegistry, WQFunction

# Arithmetic
@OperatorRegistry.register()
class add(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class subtract(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class signed_power(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class reverse(WQFunction):
    nargs = (1,)

@OperatorRegistry.register()
class multiply(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class densify(WQFunction):
    nargs = (1,)

@OperatorRegistry.register()
class divide(WQFunction):
    nargs = (2,)
