
from sqlalchemy import select, and_, bindparam, case
from sqlalchemy.sql.expression import func

from ampel.ztf.archive.ArchiveDB import ArchiveDB

class ZUDSArchiveDB(ArchiveDB):
    """
    """
    def get_alert_id_column(self):
        return self._meta.tables['candidate'].c.candid

    def _get_alert_column(self, name):
        if name == 'jd':
            jdstart = super()._get_alert_column('jdstartstack')
            jd = super()._get_alert_column('jd')
            alert_type = super()._get_alert_column('alert_type')
            return case([(alert_type=='single', jd)], else_=jdstart)
        else:
            return super()._get_alert_column(name)

    @classmethod
    def _build_queries(cls, meta):
        """ """
        from sqlalchemy.sql import null
        from sqlalchemy.sql.functions import array_agg

        Photopoint = meta.tables['photopoint']
        Candidate = meta.tables['candidate']
        Pivot = meta.tables['alert_photopoint_pivot']
        Cutout = meta.tables['cutout']
        unnest = func.unnest

        def without_keys(table):
            keys = set(table.primary_key.columns)
            for fk in table.foreign_keys:
                keys.update(fk.constraint.columns)
            return [c for c in table.columns if c not in keys]

        alert_query = Candidate.select()

        # build a query for detections
        bridge = select(
            [Pivot.c.candid, unnest(Pivot.c.photopoint_id).label('photopoint_id')]
        ).alias('bridge')
        history_query = select(Photopoint.columns).select_from(
            Photopoint.join(
                bridge, Photopoint.c.id == bridge.c.photopoint_id
            )
        ).where(
            bridge.c.candid == bindparam('alert_id')
        )

        cutout_query = select(
            [Cutout.c.kind, Cutout.c.stampData]
        ).where(
            Cutout.c.candid == bindparam('alert_id')
        )

        return alert_query, history_query, cutout_query

    def _construct_alert(self, candidate_row, with_history=True, with_cutouts=True):
        candidate = dict(candidate_row)
        candid = candidate['candid']
        alert = {'candid': candidate['candid'], 'publisher': 'ampel'}

        for k in 'objectId', 'schemavsn':
            alert[k] = candidate.pop(k)

        bad_keys = ['inserted_at']
        if candidate['alert_type'] == 'single':
            bad_keys += ['jdstartstack', 'jdendstack', 'nframesstack', 'jdmed']
        else:
            bad_keys += ['diffmaglim', 'jd', 'nid']
        for k in bad_keys:
            del candidate[k]
        alert['candidate'] = {'programpi': None, 'pdiffimfilename': None, **candidate}

        alert['light_curve'] = []

        if with_history:
            for result in self._connection.execute(self._history_query, alert_id=candid):
                alert['light_curve'].append(result)

        if with_cutouts:
            for result in self._connection.execute(self._cutout_query, alert_id=candid):
                alert['cutout{}'.format(result['kind'].title())] = result['stampData']

        return alert
    
 