class ScoreOperator:
    op_name: str
    def apply(self, series_df, spec):
        raise NotImplementedError
