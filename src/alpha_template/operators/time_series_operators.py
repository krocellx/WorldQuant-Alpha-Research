from src.alpha_template.operator_registry import OperatorRegistry, WQFunction

@OperatorRegistry.register()
class ts_mean(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class ts_rank(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class ts_zscore(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class ts_delta(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class decay_linear(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class ts_decay_linear(WQFunction):
    nargs = None  # allows variable-length args

    @classmethod
    def eval(cls, *args):
        # Optionally normalize or check arguments
        return None

@OperatorRegistry.register()
class ts_backfill(WQFunction):
    nargs = None  # Allow variable number of arguments

    @classmethod
    def eval(cls, *args, **kwargs):
        # You can implement actual logic if needed later
        return None

@OperatorRegistry.register()
class ts_std_dev(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class ts_sum(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class ts_delay(WQFunction):
    nargs = (2,)

@OperatorRegistry.register()
class ts_regression(WQFunction):
    nargs = None  # Allow variable number of arguments

    @classmethod
    def eval(cls, *args, **kwargs):
        # You can implement actual logic if needed later
        return None