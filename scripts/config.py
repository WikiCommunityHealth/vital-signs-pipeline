import os

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASS = os.getenv('POSTGRES_PASSWORD')

db_uri_editors = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}@postgres-backend/vital_signs_editors'
db_uri_web = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}@postgres-backend/vital_signs_web'
dumps_path = '/opt/airflow/mediawiki_history/'



wikilanguagecodes = ['ab', 'ace', 'ady', 'af', 'ak', 'als', 'alt', 'ami', 'am', 'ang', 'ann', 'anp', 'an', 'arc', 'ar', 'ary', 'arz', 'ast', 'as', 'atj',
                     'avk', 'av', 'awa', 'ay', 'azb', 'az', 'ban', 'bar', 'bat_smg', 'ba', 'bbc', 'bcl', 'bdr', 'be_x_old', 'be', 'bew', 'bg', 'bh', 'bi', 'bjn',
                     'blk', 'bm', 'bn', 'bo', 'bpy', 'br', 'bs', 'btm', 'bug', 'bxr', 'ca', 'cbk_zam', 'cdo', 'ceb', 'ce', 'cho', 'chr', 'ch', 'chy', 'ckb',
                     'co', 'crh', 'cr', 'csb', 'cs', 'cu', 'cv', 'cy', 'dag', 'da', 'de', 'dga', 'din', 'diq', 'dsb', 'dtp', 'dty', 'dv', 'dz', 'ee', 'el',
                     'eml', 'en', 'eo', 'es', 'et', 'eu', 'ext', 'fat', 'fa', 'ff', 'fiu_vro', 'fi', 'fj', 'fon', 'fo', 'frp', 'frr', 'fr', 'fur', 'fy',
                     'gag', 'gan', 'ga', 'gcr', 'gd', 'glk', 'gl', 'gn', 'gom', 'gor', 'got', 'gpe', 'guc', 'gur', 'gu', 'guw', 'gv', 'hak', 'ha', 'haw', 'he', 'hif',
                           'hi', 'ho', 'hr', 'hsb', 'ht', 'hu', 'hy', 'hyw', 'hz', 'ia', 'iba', 'id', 'ie', 'igl', 'ig', 'ii', 'ik', 'ilo', 'inh', 'io', 'is', 'it',
                     'iu', 'jam', 'ja', 'jbo', 'jv', 'kaa', 'kab', 'ka', 'kbd', 'kbp', 'kcg', 'kge', 'kg', 'ki', 'kj', 'kk', 'kl', 'km', 'knc', 'kn', 'koi', 'ko', 'krc', 'kr', 'ksh',
                     'ks', 'kus', 'ku', 'kv', 'kw', 'ky', 'labs', 'lad', 'la', 'lbe', 'lb', 'lez', 'lfn', 'lg', 'lij', 'li', 'lld', 'lmo', 'ln',
                     'lo', 'lrc', 'ltg', 'lt', 'lv', 'mad', 'mai', 'map_bms', 'mdf', 'meta', 'mg', 'mhr', 'mh', 'min', 'mi', 'mk', 'ml',
                     'mni', 'mn', 'mnw', 'mos', 'mrj', 'mr', 'ms', 'mt', 'mus', 'mwl', 'myv', 'my', 'mzn', 'nah', 'nap', 'na', 'nds_nl', 'nds', 'ne', 'new', 'ng',
                     'nia', 'nl', 'nn', 'nov', 'no', 'nqo', 'nrm', 'nr', 'nso', 'nv', 'ny', 'oc', 'olo', 'om', 'or', 'os', 'pag', 'pam', 'pap', 'pa', 'pcd',
                     'pcm', 'pdc', 'pfl', 'pih', 'pi', 'pl', 'pms', 'pnb', 'pnt', 'ps', 'pt', 'pwn', 'qu', 'rm', 'rmy', 'rn', 'roa_rup', 'roa_tara', 'ro', 'rsk', 'rue', 'ru', 'rw',
                     'sah', 'sat', 'sa', 'scn', 'sco', 'sc', 'sd', 'se', 'sg', 'shi', 'shn', 'sh', 'simple', 'si', 'skr', 'sk', 'sl', 'smn', 'sm', 'sn', 'so', 'sq',
                     'srn', 'sr', 'ss', 'stq', 'st', 'su', 'sv', 'sw', 'syl', 'szl', 'szy', 'ta', 'tay', 'tcy', 'tdd', 'ten', 'tet', 'te', 'tg', 'th', 'tig',
                     'ti', 'tk', 'tl', 'tly', 'tn', 'to', 'tpi', 'trv', 'tr', 'ts', 'tt', 'tum', 'tw', 'tyv', 'ty', 'udm', 'ug', 'uk', 'ur', 'uz', 'vec', 'vep', 've', 'vi', 'vls', 'vote', 'vo', 'war',
                     'wa', 'wo', 'wuu', 'xal', 'xh', 'xmf', 'yi', 'yo', 'za', 'zea', 'zgh', 'zh_classical', 'zh_min_nan', 'zh_yue', 'zh', 'zu']

