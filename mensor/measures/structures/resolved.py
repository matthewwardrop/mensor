import re

from .features import Feature


class ResolvedFeature:

    __slots__ = ('feature', 'props')

    def __init__(self, feature, **props):
        self.feature = feature
        self.props = props

    # Feature properties
    def __getattr__(self, attr):
        return getattr(self.feature, attr)

    # Property handling
    def with_props(self, **props):
        new = self.__class__(
            self.feature,
            **{
                **self.props
            }
        )
        for prop, value in props.items():
            setattr(new, prop, value)
        return new

    # TODO: remove this?
    def resolve(self, **props):
        return self.with_props(**props)

    @property
    def mask(self):
        return self.props.get('mask') or self.name

    @mask.setter
    def mask(self, mask):
        if mask is not None and not re.match(r'^(?![0-9])[\w\._:]+$', mask):
            raise ValueError("Invalid feature mask '{}'. All masks must consist only of word characters, numbers, underscores and colons, and cannot start with a number.".format(mask))
        self.props['mask'] = mask

    def with_mask(self, mask):
        return self.with_props(mask=mask or None)

    @property
    def transforms(self):
        return self.props.get('transforms', {})

    @transforms.setter
    def transforms(self, transforms):
        # TODO: Check structure of transforms dict
        if not transforms:
            self.props['transforms'] = {}
        else:
            self.props['transforms'] = transforms

    @property
    def external(self):
        return self.props.get('external', False)

    @external.setter
    def external(self, value):
        self.props['external'] = value

    @property
    def as_external(self):
        return self.with_props(external=True)

    @property
    def as_internal(self):
        return self.with_props(external=False)

    @property
    def private(self):
        return self.props.get('private', False)

    @private.setter
    def private(self, value):
        self.props['private'] = value

    @property
    def as_private(self):
        return self.with_props(private=True)

    @property
    def as_public(self):
        return self.with_props(private=False)

    @property
    def implicit(self):
        return self.props.get('implicit', False)

    @implicit.setter
    def implicit(self, value):
        self.props['implicit'] = value

    @property
    def as_implicit(self):
        return self.with_props(implicit=True)

    @property
    def as_explicit(self):
        return self.with_props(implicit=True)

    @property
    def via(self):
        return self.props.get('via')

    @via.setter
    def via(self, via):
        self.props['via'] = via or None

    def as_via(self, *vias):
        vias = [via.name if isinstance(via, Feature) else via for via in vias]
        # In the case that we are adding a single via, there cannot be two identical components
        # in a row (patterns can repeat in some instances). To simplify code
        # elsewhere, we suppress via in the case that len(vias) == 1 and the provided
        # via is alrady in the via path.
        current_vias = self.via.split('/') if self.via else []
        if len(vias) == 1 and (not vias[0] or len(current_vias) > 0 and vias[0] == current_vias[-1]):
            return self
        return self.with_props(via='/'.join(vias + current_vias))

    @property
    def via_next(self):
        s = self.via.split('/')
        if len(s) > 0:
            return self.with_props(unit_type=s[0], via='/'.join(s[1:]))
        return None

    @property
    def unit_type(self):
        # TODO: Pre-populate if identifier is nested feature type?
        return self.props.get('unit_type')

    @unit_type.setter
    def unit_type(self, unit_type):
        self.props['unit_type'] = unit_type

    @property
    def next_unit_type(self):
        if self.via:
            return self.via.split('/')[0]

    # TODO: Use this hash to allow multiple measures based on same source measure
    # @property
    # def attr_hash(self):
    #     if self.alias == self.mask:
    #         return
    #
    #     return hashlib.sha256(
    #         json.dumps(
    #             {
    #                 'alias': self.alias,
    #                 **self._extra_attributes
    #             }, sort_keys=True
    #         )
    #         .encode()
    #     ).hexdigest()

    @property
    def via_name(self):
        # TODO: Use this hash to allow multiple measures based on same source measure?
        # hash_suffix = ("_{}".format(self.attr_hash) if self.attr_hash else '')
        if self.via:
            return '{}/{}'.format(self.via, self.mask)
        return self.mask

    def via_alias(self, unit_type=None):
        if not self.transforms:
            return self.via_name
        if unit_type and unit_type in self.transforms and self.transforms[unit_type].get('alias'):
            return self.transforms[unit_type].get('alias')

        if self.via:
            unit_types = self.via.split('/')
            for i, unit_type in enumerate(unit_types):
                if unit_type in self.transforms and self.transforms[unit_type].get('alias'):
                    return '/'.join(unit_types[:i + 1] + [self.transforms[unit_type].get('alias')])

        return self.via_name

    # Methods to assist MeasureProviders with handling data field names
    def fieldname(self, role=None, unit_type=None):
        name = self.via_alias(unit_type=unit_type)
        if role == 'measure':
            return f'{name}|raw'
        return name

    def prev_fieldname(self, role=None):
        if self.via:
            return "/".join([self.next_unit_type, self.via_next.fieldname(role=role, unit_type=self.next_unit_type)])

    def __lt__(self, other):  # TODO: Where is this used?
        return self.name.__lt__(other.name)

    def __repr__(self):
        attrs = []
        for attr in ['external', 'private', 'implicit']:
            if getattr(self, attr, False):
                attrs.append(attr[0])
        return (
            f'Resolved<{self.feature} as '
            + self.via_name
            + ('[{}]'.format(self.name) if self.mask != self.name else '')
            + ('({})'.format(','.join(attrs)) if attrs else '')
            + '>'
        )

    def __hash__(self):
        return hash(self.mask)

    def __eq__(self, other):
        if isinstance(other, str):
            return self.mask == other
        if isinstance(other, Feature):
            return self.name == other.name and type(self.feature[0]) == type(other)  # TODO: fix
        return False

    # TO clean up and merge into above

    def transforms_for_unit_type(self, unit_type, stats_registry=None):
        transforms = {
            'pre_agg': None,
            'agg': 'sum',
            'post_agg': None,
            'pre_rebase_agg': None,
            'rebase_agg': 'sum',
            'post_rebase_agg': None
        }
        if isinstance(self.transforms, dict):
            transforms.update(self.transforms.get(unit_type, {}))

        backend_aggs = stats_registry.aggregations.for_provider(self.provider)
        transform_ops = stats_registry.transforms.for_provider(self.provider)

        for key in ['agg', 'rebase_agg']:
            if transforms[key] is not None:
                transforms[key] = backend_aggs[transforms[key]]

        for key in ['pre_agg', 'post_agg', 'pre_rebase_agg', 'post_rebase_agg']:
            if transforms[key] is not None:
                transforms[key] = transform_ops[transforms[key]]

        return transforms

    def get_fields(self, unit_type=None, stats=True, rebase_agg=False, stats_registry=None, for_pandas=False):
        """
        This is a convenience method for subclasses to use to get the
        target fields associated with a particular distribution.

        Args:
            stats (bool): Whether this measure is being aggregated into
                distribution statistics.

        Returns:
            OrderedDict: A mapping of field suffixes to agg methods to collect
                in order to reproduce the distribution from which a measure was
                sampled.
        """
        assert stats_registry is not None
        assert not (rebase_agg and stats)
        if for_pandas:
            from mensor.backends.pandas import PandasMeasureProvider
            provider = PandasMeasureProvider
        else:
            provider = self.provider

        if stats:
            transforms = self.transforms_for_unit_type(unit_type, stats_registry=stats_registry)
            return OrderedDict([
                (
                    (
                        "{via_name}|{field_name}".format(via_name=self.fieldname(role=None, unit_type=unit_type if not rebase_agg else None), field_name=field_name)
                        if self.distribution is None else
                        "{via_name}|{dist_name}|{field_name}".format(via_name=self.fieldname(role=None, unit_type=unit_type if not rebase_agg else None), field_name=field_name, dist_name=self.distribution.lower())
                    ),
                    {
                        'pre_agg': transforms['pre_agg'],
                        'agg': agg_method
                    }
                )
                for field_name, agg_method in stats_registry.distribution_for_provider(self.distribution, provider).items()
            ])
        else:
            transforms = self.transforms_for_unit_type(unit_type, stats_registry=stats_registry)
            return OrderedDict([
                (
                    '{fieldname}|raw'.format(fieldname=self.fieldname(role=None, unit_type=unit_type if not rebase_agg else None)),
                    {
                        'agg': transforms['rebase_agg'] if rebase_agg else transforms['agg'],
                        'pre_agg': transforms['pre_rebase_agg'] if rebase_agg else transforms['pre_agg'],
                        'post_agg': transforms['post_rebase_agg'] if rebase_agg else transforms['post_agg'],
                    }
                )
            ])

    @classmethod
    def get_all_fields(self, measures, unit_type=None, stats=True, rebase_agg=False, stats_registry=None, for_pandas=False):
        fields = []
        for measure in measures:
            fields.extend(measure.get_fields(unit_type=unit_type, stats=stats, rebase_agg=rebase_agg, stats_registry=stats_registry, for_pandas=for_pandas))
        return fields
