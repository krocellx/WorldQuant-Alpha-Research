from src.alpha_template.operator_registry import OperatorRegistry, WQFunction

@OperatorRegistry.register()
class trade_when(WQFunction):
    nargs = (3,)  # signal, condition

@OperatorRegistry.register()
class bucket(WQFunction):
    nargs = None  # variable-length args

    @classmethod
    def eval(cls, *args, **kwargs):
        # You can parse kwargs like 'range' or 'buckets' here
        return None
