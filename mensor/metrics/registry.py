

class MetricRegistry(object):

    def __init__(self, measureRegistry):
        self.measures = measureRegistry

    def register(self, metric_name, measure_expr):
        pass

    def evaluate(self, *metrics, conditions=None, segment_by=None, **opts):
        pass
