class Join(object):

    # TODO: Review Join API (esp. which arguments are essential, etc)

    def __init__(self, provider, unit_type, left_on, right_on, object,
                 compatible=False, join_prefix=None, name=None, measures=None, dimensions=None,
                 how='left'):
        self.provider = provider
        self.unit_type = unit_type
        self.join_prefix = join_prefix
        self.left_on = left_on
        self.right_on = right_on
        self.name = name
        self.measures = measures
        self.dimensions = dimensions
        self.object = object
        self.compatible = compatible
        self.how = how

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if name is None:
            name = "join_{}_{}".format(self.provider.name, self.unit_type.name)
        self._name = name
