def startseq_match(A, B):
    '''
    Checks whether sequence a starts sequence b.
    For example: startseq_match([1,2], [1,2,3]) == True.
    '''
    for i, a in enumerate(A):
        if i == len(B) or a != B[i]:
            return False
    return True


class AttrDict(dict):

    def __dir__(self):
        return dict.__dir__(self) + self.keys()

    def __getattr__(self, key):
        return self[key]

    def map(self, names):
        return [self[name] for name in names]
