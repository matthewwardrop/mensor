import yaml


class FeatureSpec:

    ALLOWED_TRANSFORMS = ['pre_agg', 'agg', 'post_agg', 'pre_rebase_agg', 'rebase_agg', 'post_rebase_agg', 'alias']

    def __init__(self, alias, source=None, transforms=None):
        self._alias = alias
        self._source = source or alias
        self._transforms = transforms or {}

    def __repr__(self):
        return "<Measure" + ("[{alias}]".format(alias=self._alias) if self._alias else "") + (" (with transforms)" if self._transforms else "") + ">"

    def source(self, source):
        self._source = source
        return self

    def with_transforms(self, unit_type, pre_agg=None, agg=None, post_agg=None, pre_rebase_agg=None, rebase_agg=None, post_rebase_agg=None, alias=None):
        variables = locals()
        transforms = {
            name: variables[name]
            for name in self.ALLOWED_TRANSFORMS
            if variables[name] is not None
        }
        if transforms:
            self._transforms[unit_type] = transforms
        return self

    def get_attrs(self, unit_type):
        attrs = {
            'transforms': self._transforms
        }
        if unit_type not in attrs['transforms']:
            attrs['transforms'][unit_type] = {}
        attrs['transforms'][unit_type]['alias'] = self._alias
        return attrs

    def as_source_with_attrs(self, unit_type):
        return self._source, self.get_attrs(unit_type)

    @property
    def as_dict(self):
        return {
            'source': self._source,
            'alias': self._alias,
            'transforms': self._transforms
        }

    @property
    def as_yaml(self):
        return yaml.dump(self.as_dict, default_flow_style=False, indent=4)


class MeasureSpec(FeatureSpec):

    ALLOWED_TRANSFORMS = ['pre_agg', 'agg', 'post_agg', 'pre_rebase_agg', 'rebase_agg', 'post_rebase_agg', 'alias']


class DimensionSpec(FeatureSpec):

    ALLOWED_TRANSFORMS = ['pre_agg', 'pre_rebase_agg', 'alias']
