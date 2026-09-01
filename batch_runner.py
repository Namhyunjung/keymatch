"""
batch_runner.py
cron이든 Airflow든, 이 스크립트 하나를 실행하면 전체 파이프라인이 돈다.
    python batch_runner.py --mode full        # 전체 재계산 (신규 지역 백필용, 201705~ 전체월 API 호출)
    python batch_runner.py --mode incremental # 최근 3개월만 (매일 새벽 배치)

EXTRACT는 AptTradeConnector로 실 API 호출. .env의 APT_API_KEY 필요.
DB는 Supabase(Postgres). .env의 DATABASE_URL 필요 (db.py 참고).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

import db
from apt_trade_connector import AptTradeConnector, clean_transactions, build_monthly_price
from kapt_connector import HouseholdsResolver
from sync_engine import AlgoParams, Regime, evaluate_candidate, compute_regime_return, pick_leader

load_dotenv()

# 법정동코드 (행정표준코드관리시스템 code.go.kr 조회, 2026-08 기준 "존재" 상태만).
# 구조: LAWD_CD(5자리, MOLIT API 호출용) -> {sido, sigungu, region_code(구 자체, 10자리),
#       dongs: {동/읍/면명 -> region_code(10자리)}}. API 응답의 umdNm으로 단지를
# 정확한 동에 매핑하는 데 씀. 동명이 구를 넘어 겹칠 수 있어(예: '중동'이 중원구/기흥구에
# 둘 다 있음) lawd_cd까지 같이 봐야 함 — transform_and_load 참고.
REGIONS = {
    '11680': {
        'sido': '서울', 'sigungu': '강남구', 'region_code': '1168000000',
        'dongs': {
            '역삼동': '1168010100', '개포동': '1168010300', '청담동': '1168010400',
            '삼성동': '1168010500', '대치동': '1168010600', '신사동': '1168010700',
            '논현동': '1168010800', '압구정동': '1168011000', '세곡동': '1168011100',
            '자곡동': '1168011200', '율현동': '1168011300', '일원동': '1168011400',
            '수서동': '1168011500', '도곡동': '1168011800',
        },
    },
    '11110': {
        'sido': '서울', 'sigungu': '종로구', 'region_code': '1111000000',
        'dongs': {
            '청운동': '1111010100', '신교동': '1111010200', '궁정동': '1111010300',
            '효자동': '1111010400', '창성동': '1111010500', '통의동': '1111010600',
            '적선동': '1111010700', '통인동': '1111010800', '누상동': '1111010900',
            '누하동': '1111011000', '옥인동': '1111011100', '체부동': '1111011200',
            '필운동': '1111011300', '내자동': '1111011400', '사직동': '1111011500',
            '도렴동': '1111011600', '당주동': '1111011700', '내수동': '1111011800',
            '세종로': '1111011900', '신문로1가': '1111012000', '신문로2가': '1111012100',
            '청진동': '1111012200', '서린동': '1111012300', '수송동': '1111012400',
            '중학동': '1111012500', '종로1가': '1111012600', '공평동': '1111012700',
            '관훈동': '1111012800', '견지동': '1111012900', '와룡동': '1111013000',
            '권농동': '1111013100', '운니동': '1111013200', '익선동': '1111013300',
            '경운동': '1111013400', '관철동': '1111013500', '인사동': '1111013600',
            '낙원동': '1111013700', '종로2가': '1111013800', '팔판동': '1111013900',
            '삼청동': '1111014000', '안국동': '1111014100', '소격동': '1111014200',
            '화동': '1111014300', '사간동': '1111014400', '송현동': '1111014500',
            '가회동': '1111014600', '재동': '1111014700', '계동': '1111014800',
            '원서동': '1111014900', '훈정동': '1111015000', '묘동': '1111015100',
            '봉익동': '1111015200', '돈의동': '1111015300', '장사동': '1111015400',
            '관수동': '1111015500', '종로3가': '1111015600', '인의동': '1111015700',
            '예지동': '1111015800', '원남동': '1111015900', '연지동': '1111016000',
            '종로4가': '1111016100', '효제동': '1111016200', '종로5가': '1111016300',
            '종로6가': '1111016400', '이화동': '1111016500', '연건동': '1111016600',
            '충신동': '1111016700', '동숭동': '1111016800', '혜화동': '1111016900',
            '명륜1가': '1111017000', '명륜2가': '1111017100', '명륜4가': '1111017200',
            '명륜3가': '1111017300', '창신동': '1111017400', '숭인동': '1111017500',
            '교남동': '1111017600', '평동': '1111017700', '송월동': '1111017800',
            '홍파동': '1111017900', '교북동': '1111018000', '행촌동': '1111018100',
            '구기동': '1111018200', '평창동': '1111018300', '부암동': '1111018400',
            '홍지동': '1111018500', '신영동': '1111018600', '무악동': '1111018700',
        },
    },
    '11140': {
        'sido': '서울', 'sigungu': '중구', 'region_code': '1114000000',
        'dongs': {
            '무교동': '1114010100', '다동': '1114010200', '태평로1가': '1114010300',
            '을지로1가': '1114010400', '을지로2가': '1114010500', '남대문로1가': '1114010600',
            '삼각동': '1114010700', '수하동': '1114010800', '장교동': '1114010900',
            '수표동': '1114011000', '소공동': '1114011100', '남창동': '1114011200',
            '북창동': '1114011300', '태평로2가': '1114011400', '남대문로2가': '1114011500',
            '남대문로3가': '1114011600', '남대문로4가': '1114011700', '남대문로5가': '1114011800',
            '봉래동1가': '1114011900', '봉래동2가': '1114012000', '회현동1가': '1114012100',
            '회현동2가': '1114012200', '회현동3가': '1114012300', '충무로1가': '1114012400',
            '충무로2가': '1114012500', '명동1가': '1114012600', '명동2가': '1114012700',
            '남산동1가': '1114012800', '남산동2가': '1114012900', '남산동3가': '1114013000',
            '저동1가': '1114013100', '충무로4가': '1114013200', '충무로5가': '1114013300',
            '인현동2가': '1114013400', '예관동': '1114013500', '묵정동': '1114013600',
            '필동1가': '1114013700', '필동2가': '1114013800', '필동3가': '1114013900',
            '남학동': '1114014000', '주자동': '1114014100', '예장동': '1114014200',
            '장충동1가': '1114014300', '장충동2가': '1114014400', '광희동1가': '1114014500',
            '광희동2가': '1114014600', '쌍림동': '1114014700', '을지로6가': '1114014800',
            '을지로7가': '1114014900', '을지로4가': '1114015000', '을지로5가': '1114015100',
            '주교동': '1114015200', '방산동': '1114015300', '오장동': '1114015400',
            '을지로3가': '1114015500', '입정동': '1114015600', '산림동': '1114015700',
            '충무로3가': '1114015800', '초동': '1114015900', '인현동1가': '1114016000',
            '저동2가': '1114016100', '신당동': '1114016200', '흥인동': '1114016300',
            '무학동': '1114016400', '황학동': '1114016500', '서소문동': '1114016600',
            '정동': '1114016700', '순화동': '1114016800', '의주로1가': '1114016900',
            '충정로1가': '1114017000', '중림동': '1114017100', '의주로2가': '1114017200',
            '만리동1가': '1114017300', '만리동2가': '1114017400',
        },
    },
    '11170': {
        'sido': '서울', 'sigungu': '용산구', 'region_code': '1117000000',
        'dongs': {
            '후암동': '1117010100', '용산동2가': '1117010200', '용산동4가': '1117010300',
            '갈월동': '1117010400', '남영동': '1117010500', '용산동1가': '1117010600',
            '동자동': '1117010700', '서계동': '1117010800', '청파동1가': '1117010900',
            '청파동2가': '1117011000', '청파동3가': '1117011100', '원효로1가': '1117011200',
            '원효로2가': '1117011300', '신창동': '1117011400', '산천동': '1117011500',
            '청암동': '1117011600', '원효로3가': '1117011700', '원효로4가': '1117011800',
            '효창동': '1117011900', '도원동': '1117012000', '용문동': '1117012100',
            '문배동': '1117012200', '신계동': '1117012300', '한강로1가': '1117012400',
            '한강로2가': '1117012500', '용산동3가': '1117012600', '용산동5가': '1117012700',
            '한강로3가': '1117012800', '이촌동': '1117012900', '이태원동': '1117013000',
            '한남동': '1117013100', '동빙고동': '1117013200', '서빙고동': '1117013300',
            '주성동': '1117013400', '용산동6가': '1117013500', '보광동': '1117013600',
        },
    },
    '11200': {
        'sido': '서울', 'sigungu': '성동구', 'region_code': '1120000000',
        'dongs': {
            '상왕십리동': '1120010100', '하왕십리동': '1120010200', '홍익동': '1120010300',
            '도선동': '1120010400', '마장동': '1120010500', '사근동': '1120010600',
            '행당동': '1120010700', '응봉동': '1120010800', '금호동1가': '1120010900',
            '금호동2가': '1120011000', '금호동3가': '1120011100', '금호동4가': '1120011200',
            '옥수동': '1120011300', '성수동1가': '1120011400', '성수동2가': '1120011500',
            '송정동': '1120011800', '용답동': '1120012200',
        },
    },
    '11215': {
        'sido': '서울', 'sigungu': '광진구', 'region_code': '1121500000',
        'dongs': {
            '중곡동': '1121510100', '능동': '1121510200', '구의동': '1121510300',
            '광장동': '1121510400', '자양동': '1121510500', '화양동': '1121510700',
            '군자동': '1121510900',
        },
    },
    '11230': {
        'sido': '서울', 'sigungu': '동대문구', 'region_code': '1123000000',
        'dongs': {
            '신설동': '1123010100', '용두동': '1123010200', '제기동': '1123010300',
            '전농동': '1123010400', '답십리동': '1123010500', '장안동': '1123010600',
            '청량리동': '1123010700', '회기동': '1123010800', '휘경동': '1123010900',
            '이문동': '1123011000',
        },
    },
    '11260': {
        'sido': '서울', 'sigungu': '중랑구', 'region_code': '1126000000',
        'dongs': {
            '면목동': '1126010100', '상봉동': '1126010200', '중화동': '1126010300',
            '묵동': '1126010400', '망우동': '1126010500', '신내동': '1126010600',
        },
    },
    '11290': {
        'sido': '서울', 'sigungu': '성북구', 'region_code': '1129000000',
        'dongs': {
            '성북동': '1129010100', '성북동1가': '1129010200', '돈암동': '1129010300',
            '동소문동1가': '1129010400', '동소문동2가': '1129010500', '동소문동3가': '1129010600',
            '동소문동4가': '1129010700', '동소문동5가': '1129010800', '동소문동6가': '1129010900',
            '동소문동7가': '1129011000', '삼선동1가': '1129011100', '삼선동2가': '1129011200',
            '삼선동3가': '1129011300', '삼선동4가': '1129011400', '삼선동5가': '1129011500',
            '동선동1가': '1129011600', '동선동2가': '1129011700', '동선동3가': '1129011800',
            '동선동4가': '1129011900', '동선동5가': '1129012000', '안암동1가': '1129012100',
            '안암동2가': '1129012200', '안암동3가': '1129012300', '안암동4가': '1129012400',
            '안암동5가': '1129012500', '보문동4가': '1129012600', '보문동5가': '1129012700',
            '보문동6가': '1129012800', '보문동7가': '1129012900', '보문동1가': '1129013000',
            '보문동2가': '1129013100', '보문동3가': '1129013200', '정릉동': '1129013300',
            '길음동': '1129013400', '종암동': '1129013500', '하월곡동': '1129013600',
            '상월곡동': '1129013700', '장위동': '1129013800', '석관동': '1129013900',
        },
    },
    '11305': {
        'sido': '서울', 'sigungu': '강북구', 'region_code': '1130500000',
        'dongs': {
            '미아동': '1130510100', '번동': '1130510200', '수유동': '1130510300',
            '우이동': '1130510400',
        },
    },
    '11320': {
        'sido': '서울', 'sigungu': '도봉구', 'region_code': '1132000000',
        'dongs': {
            '쌍문동': '1132010500', '방학동': '1132010600', '창동': '1132010700',
            '도봉동': '1132010800',
        },
    },
    '11350': {
        'sido': '서울', 'sigungu': '노원구', 'region_code': '1135000000',
        'dongs': {
            '월계동': '1135010200', '공릉동': '1135010300', '하계동': '1135010400',
            '상계동': '1135010500', '중계동': '1135010600',
        },
    },
    '11380': {
        'sido': '서울', 'sigungu': '은평구', 'region_code': '1138000000',
        'dongs': {
            '수색동': '1138010100', '녹번동': '1138010200', '불광동': '1138010300',
            '갈현동': '1138010400', '구산동': '1138010500', '대조동': '1138010600',
            '응암동': '1138010700', '역촌동': '1138010800', '신사동': '1138010900',
            '증산동': '1138011000', '진관동': '1138011400',
        },
    },
    '11410': {
        'sido': '서울', 'sigungu': '서대문구', 'region_code': '1141000000',
        'dongs': {
            '충정로2가': '1141010100', '충정로3가': '1141010200', '합동': '1141010300',
            '미근동': '1141010400', '냉천동': '1141010500', '천연동': '1141010600',
            '옥천동': '1141010700', '영천동': '1141010800', '현저동': '1141010900',
            '북아현동': '1141011000', '홍제동': '1141011100', '대현동': '1141011200',
            '대신동': '1141011300', '신촌동': '1141011400', '봉원동': '1141011500',
            '창천동': '1141011600', '연희동': '1141011700', '홍은동': '1141011800',
            '북가좌동': '1141011900', '남가좌동': '1141012000',
        },
    },
    '11440': {
        'sido': '서울', 'sigungu': '마포구', 'region_code': '1144000000',
        'dongs': {
            '아현동': '1144010100', '공덕동': '1144010200', '신공덕동': '1144010300',
            '도화동': '1144010400', '용강동': '1144010500', '토정동': '1144010600',
            '마포동': '1144010700', '대흥동': '1144010800', '염리동': '1144010900',
            '노고산동': '1144011000', '신수동': '1144011100', '현석동': '1144011200',
            '구수동': '1144011300', '창전동': '1144011400', '상수동': '1144011500',
            '하중동': '1144011600', '신정동': '1144011700', '당인동': '1144011800',
            '서교동': '1144012000', '동교동': '1144012100', '합정동': '1144012200',
            '망원동': '1144012300', '연남동': '1144012400', '성산동': '1144012500',
            '중동': '1144012600', '상암동': '1144012700',
        },
    },
    '11470': {
        'sido': '서울', 'sigungu': '양천구', 'region_code': '1147000000',
        'dongs': {
            '신정동': '1147010100', '목동': '1147010200', '신월동': '1147010300',
        },
    },
    '11500': {
        'sido': '서울', 'sigungu': '강서구', 'region_code': '1150000000',
        'dongs': {
            '염창동': '1150010100', '등촌동': '1150010200', '화곡동': '1150010300',
            '가양동': '1150010400', '마곡동': '1150010500', '내발산동': '1150010600',
            '외발산동': '1150010700', '공항동': '1150010800', '방화동': '1150010900',
            '개화동': '1150011000', '과해동': '1150011100', '오곡동': '1150011200',
            '오쇠동': '1150011300',
        },
    },
    '11530': {
        'sido': '서울', 'sigungu': '구로구', 'region_code': '1153000000',
        'dongs': {
            '신도림동': '1153010100', '구로동': '1153010200', '가리봉동': '1153010300',
            '고척동': '1153010600', '개봉동': '1153010700', '오류동': '1153010800',
            '궁동': '1153010900', '온수동': '1153011000', '천왕동': '1153011100',
            '항동': '1153011200',
        },
    },
    '11545': {
        'sido': '서울', 'sigungu': '금천구', 'region_code': '1154500000',
        'dongs': {
            '가산동': '1154510100', '독산동': '1154510200', '시흥동': '1154510300',
        },
    },
    '11560': {
        'sido': '서울', 'sigungu': '영등포구', 'region_code': '1156000000',
        'dongs': {
            '영등포동': '1156010100', '영등포동1가': '1156010200', '영등포동2가': '1156010300',
            '영등포동3가': '1156010400', '영등포동4가': '1156010500', '영등포동5가': '1156010600',
            '영등포동6가': '1156010700', '영등포동7가': '1156010800', '영등포동8가': '1156010900',
            '여의도동': '1156011000', '당산동1가': '1156011100', '당산동2가': '1156011200',
            '당산동3가': '1156011300', '당산동4가': '1156011400', '당산동5가': '1156011500',
            '당산동6가': '1156011600', '당산동': '1156011700', '도림동': '1156011800',
            '문래동1가': '1156011900', '문래동2가': '1156012000', '문래동3가': '1156012100',
            '문래동4가': '1156012200', '문래동5가': '1156012300', '문래동6가': '1156012400',
            '양평동1가': '1156012500', '양평동2가': '1156012600', '양평동3가': '1156012700',
            '양평동4가': '1156012800', '양평동5가': '1156012900', '양평동6가': '1156013000',
            '양화동': '1156013100', '신길동': '1156013200', '대림동': '1156013300',
            '양평동': '1156013400',
        },
    },
    '11590': {
        'sido': '서울', 'sigungu': '동작구', 'region_code': '1159000000',
        'dongs': {
            '노량진동': '1159010100', '상도동': '1159010200', '상도1동': '1159010300',
            '본동': '1159010400', '흑석동': '1159010500', '동작동': '1159010600',
            '사당동': '1159010700', '대방동': '1159010800', '신대방동': '1159010900',
        },
    },
    '11620': {
        'sido': '서울', 'sigungu': '관악구', 'region_code': '1162000000',
        'dongs': {
            '봉천동': '1162010100', '신림동': '1162010200', '남현동': '1162010300',
        },
    },
    '11650': {
        'sido': '서울', 'sigungu': '서초구', 'region_code': '1165000000',
        'dongs': {
            '방배동': '1165010100', '양재동': '1165010200', '우면동': '1165010300',
            '원지동': '1165010400', '잠원동': '1165010600', '반포동': '1165010700',
            '서초동': '1165010800', '내곡동': '1165010900', '염곡동': '1165011000',
            '신원동': '1165011100',
        },
    },
    '11710': {
        'sido': '서울', 'sigungu': '송파구', 'region_code': '1171000000',
        'dongs': {
            '잠실동': '1171010100', '신천동': '1171010200', '풍납동': '1171010300',
            '송파동': '1171010400', '석촌동': '1171010500', '삼전동': '1171010600',
            '가락동': '1171010700', '문정동': '1171010800', '장지동': '1171010900',
            '방이동': '1171011100', '오금동': '1171011200', '거여동': '1171011300',
            '마천동': '1171011400',
        },
    },
    '11740': {
        'sido': '서울', 'sigungu': '강동구', 'region_code': '1174000000',
        'dongs': {
            '명일동': '1174010100', '고덕동': '1174010200', '상일동': '1174010300',
            '길동': '1174010500', '둔촌동': '1174010600', '암사동': '1174010700',
            '성내동': '1174010800', '천호동': '1174010900', '강일동': '1174011000',
        },
    },
    '41131': {
        'sido': '경기', 'sigungu': '성남시 수정구', 'region_code': '4113100000',
        'dongs': {
            '신흥동': '4113110100', '태평동': '4113110200', '수진동': '4113110300',
            '단대동': '4113110400', '산성동': '4113110500', '양지동': '4113110600',
            '복정동': '4113110700', '창곡동': '4113110800', '신촌동': '4113110900',
            '오야동': '4113111000', '심곡동': '4113111100', '고등동': '4113111200',
            '상적동': '4113111300', '둔전동': '4113111400', '시흥동': '4113111500',
            '금토동': '4113111600', '사송동': '4113111700',
        },
    },
    '41133': {
        'sido': '경기', 'sigungu': '성남시 중원구', 'region_code': '4113300000',
        'dongs': {
            '성남동': '4113310100', '금광동': '4113310300', '은행동': '4113310400',
            '상대원동': '4113310500', '여수동': '4113310600', '도촌동': '4113310700',
            '갈현동': '4113310800', '하대원동': '4113310900', '중앙동': '4113313200',
        },
    },
    '41135': {
        'sido': '경기', 'sigungu': '성남시 분당구', 'region_code': '4113500000',
        'dongs': {
            '분당동': '4113510100', '수내동': '4113510200', '정자동': '4113510300',
            '율동': '4113510400', '서현동': '4113510500', '이매동': '4113510600',
            '야탑동': '4113510700', '판교동': '4113510800', '삼평동': '4113510900',
            '백현동': '4113511000', '금곡동': '4113511100', '궁내동': '4113511200',
            '동원동': '4113511300', '구미동': '4113511400', '운중동': '4113511500',
            '대장동': '4113511600', '석운동': '4113511700', '하산운동': '4113511800',
        },
    },
    '41461': {
        'sido': '경기', 'sigungu': '용인시 처인구', 'region_code': '4146100000',
        'dongs': {
            '김량장동': '4146110100', '역북동': '4146110200', '삼가동': '4146110300',
            '남동': '4146110400', '유방동': '4146110500', '고림동': '4146110600',
            '마평동': '4146110700', '운학동': '4146110800', '호동': '4146110900',
            '해곡동': '4146111000', '포곡읍': '4146125000', '모현면': '4146131000',
            '남사면': '4146132000', '이동면': '4146133000', '원삼면': '4146134000',
            '백암면': '4146135000', '양지면': '4146136000',
        },
    },
    '41463': {
        'sido': '경기', 'sigungu': '용인시 기흥구', 'region_code': '4146300000',
        'dongs': {
            '신갈동': '4146310100', '구갈동': '4146310200', '상갈동': '4146310300',
            '하갈동': '4146310400', '보라동': '4146310500', '지곡동': '4146310600',
            '공세동': '4146310700', '고매동': '4146310800', '농서동': '4146310900',
            '서천동': '4146311000', '영덕동': '4146311100', '언남동': '4146311200',
            '마북동': '4146311300', '청덕동': '4146311400', '동백동': '4146311500',
            '중동': '4146311600', '상하동': '4146311700', '보정동': '4146311800',
        },
    },
    '41465': {
        'sido': '경기', 'sigungu': '용인시 수지구', 'region_code': '4146500000',
        'dongs': {
            '풍덕천동': '4146510100', '죽전동': '4146510200', '동천동': '4146510300',
            '고기동': '4146510400', '신봉동': '4146510500', '성복동': '4146510600',
            '상현동': '4146510700',
        },
    },
}

LAWD_CD_LIST = list(REGIONS.keys())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger('batch_runner')

SCHEMA_PATH = Path(__file__).parent / 'schema.sql'

# 마지막 국면(진행중)은 종료월을 현재월로 자동 추적 — 배치 돌 때마다 그 시점의
# "현재"로 갱신됨. 국면 자체가 바뀌었는지(정책 전환점 발생 여부)는 별도 판단 필요.
# KST 기준으로 계산 — 국내 부동산 도메인이라 UTC로 하면 KST 자정~오전 9시 사이엔
# 날짜가 하루 밀림. keymatch.html의 nowYm()도 동일하게 KST로 맞춰뒀음.
_now = datetime.now(timezone(timedelta(hours=9)))
CURRENT_YM = f"{_now.year}{_now.month:02d}"

REGIMES = [
    Regime(1, '규제강화기', '201705', '201912'),
    Regime(2, '급등기', '202001', '202204'),
    Regime(3, '하락기', '202205', '202312'),
    # 2025-06-27 "6·27 부동산대책"(수도권/규제지역 주담대 6억원 한도 최초 도입,
    # 다주택자 추가 주담대 전면금지) 전까지만 회복기로 잡음. 그 이전엔
    # 재건축 규제완화 우세 + 가격반등이었지만, 6.27대책 이후로는 성격이
    # 달라짐 — 아래 재규제기 참고.
    Regime(4, '회복기', '202401', '202506'),
    # 6.27대책(2025.06) 이후: 7월 스트레스DSR 3단계, 10.15대책으로 서울 전역+
    # 경기 12개 지역 규제지역 확대. 강남3구 거래량도 -43~45%로 꺾임 — 회복기와
    # 뚜렷이 다른 국면이라 분리함 (2026-09-01, 사용자 지적으로 재검토).
    Regime(5, '재규제기', '202507', CURRENT_YM),
]


# ------------------------------------------------------------
# DB 초기화
# ------------------------------------------------------------
def init_db(conn):
    exists = conn.execute("SELECT to_regclass('public.regions')").fetchone()[0]
    if exists is None:
        log.info("스키마 초기화")
        conn.execute(SCHEMA_PATH.read_text(encoding='utf-8'))
        conn.commit()

    # regions는 매번 실행 (ON CONFLICT DO NOTHING이라 이미 있으면 그냥 스킵) — REGIONS에
    # 새 지역을 추가해도 기존 DB에 반영되게. count==0 게이트로 1회성 시딩만 했더니
    # 성남/용인 추가 후 기존 DB엔 안 들어가서 FK 위반으로 배치가 죽은 적 있음
    # (2026-08-31 run #10, complexes_region_code_fkey).
    _seed_regions(conn)

    # pyeong_groups는 label에 유니크 제약이 없어서 매번 넣으면 중복 row가 쌓임 —
    # 이건 원래대로 비어있을 때 1회만.
    if conn.execute("SELECT COUNT(*) FROM pyeong_groups").fetchone()[0] == 0:
        log.info("pyeong_groups 시드 없음 — 시딩")
        conn.execute("INSERT INTO pyeong_groups (label, area_min, area_max) VALUES ('84㎡', 81, 88)")
        conn.commit()

    # regimes는 regions와 같은 이유로 매번 upsert — 마지막 국면(현재 진행중)의 end_ym이
    # 매달 바뀌고, 국면 자체가 새로 추가되기도 함(2026-09 재규제기 신설). count==0 게이트로
    # 1회성 시딩만 하면 코드에서 REGIMES를 바꿔도 이미 시딩된 운영 DB엔 반영이 안 됨.
    # regime_id를 SERIAL 자동채번에 맡기지 않고 REGIMES의 값을 명시해서 넣어야
    # regime_returns.regime_id FK가 항상 REGIMES 순서와 일치함이 보장됨.
    _seed_regimes(conn)


def _seed_regimes(conn):
    for r in REGIMES:
        conn.execute(
            """INSERT INTO regimes (regime_id, label, start_ym, end_ym, display_order) VALUES (%s,%s,%s,%s,%s)
               ON CONFLICT (regime_id) DO UPDATE SET
                 label=EXCLUDED.label, start_ym=EXCLUDED.start_ym,
                 end_ym=EXCLUDED.end_ym, display_order=EXCLUDED.display_order""",
            (r.regime_id, r.label, r.start_ym, r.end_ym, r.regime_id)
        )
    conn.commit()


def _seed_regions(conn):
    for r in REGIONS.values():
        conn.execute(
            """INSERT INTO regions (region_code, sido, sigungu, eupmyeondong) VALUES (%s,%s,%s,%s)
               ON CONFLICT (region_code) DO NOTHING""",
            (r['region_code'], r['sido'], r['sigungu'], None)
        )
        for dong_name, code in r['dongs'].items():
            conn.execute(
                """INSERT INTO regions (region_code, sido, sigungu, eupmyeondong) VALUES (%s,%s,%s,%s)
                   ON CONFLICT (region_code) DO NOTHING""",
                (code, r['sido'], r['sigungu'], dong_name)
            )
    conn.commit()


# ------------------------------------------------------------
# EXTRACT
# ------------------------------------------------------------
def _ym_range(start_ym: str, end_ym: str) -> list[str]:
    start = datetime.strptime(start_ym, '%Y%m')
    end = datetime.strptime(end_ym, '%Y%m')
    out = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def extract(mode: str) -> pd.DataFrame:
    """
    incremental 모드는 최근 3개월, full은 REGIMES 전체 기간(201705~) 수집.
    """
    log.info(f"EXTRACT 시작 (mode={mode})")

    if mode == 'incremental':
        now = datetime.now(timezone.utc)
        total_months = now.year * 12 + (now.month - 1) - 2  # 3개월 전
        start_y, start_m = divmod(total_months, 12)
        yms = _ym_range(f"{start_y}{start_m + 1:02d}", f"{now.year}{now.month:02d}")
    else:
        yms = _ym_range(REGIMES[0].start_ym, REGIMES[-1].end_ym)

    connector = AptTradeConnector(service_key=os.environ["APT_API_KEY"])
    raw = connector.fetch_bulk(lawd_cd_list=LAWD_CD_LIST, deal_ymd_list=yms)
    if raw.empty:
        raise RuntimeError(f"EXTRACT 결과 0행 — {yms[0]}~{yms[-1]} 전체 호출 실패(API 응답/네트워크 확인)")
    log.info(f"EXTRACT 완료 — {len(raw)}행")
    return raw


# ------------------------------------------------------------
# TRANSFORM + LOAD
# ------------------------------------------------------------
def transform_and_load(conn, raw: pd.DataFrame) -> int:
    log.info("TRANSFORM 시작")
    cleaned = clean_transactions(raw)

    # 동조판정은 같은 평형끼리만 비교 가능 — pyeong_groups 범위로 거래 필터링.
    # (전체 평형 섞어서 대표가를 뽑으면 대장단지가 엉뚱한 초대형 평형으로 튐)
    pg_id, area_min, area_max = conn.execute(
        "SELECT pyeong_group_id, area_min, area_max FROM pyeong_groups LIMIT 1"
    ).fetchone()
    area_min, area_max = float(area_min), float(area_max)
    cleaned = cleaned[cleaned['area_m2'].between(area_min, area_max)].copy()
    log.info(f"평형 필터 적용 ({area_min}~{area_max}㎡) — {len(cleaned)}행 남음")

    # 단지 마스터 upsert (apt_seq 기준)
    # 세대수는 국토부 실거래가 API에 없는 필드 — 공동주택 단지목록/기본정보 API(kapt_connector)에서
    # 단지명+준공년도(실거래가 API에서 온 진짜 값)로 매칭해서 채움. 이미 매칭된 건 재조회 안 함
    # (API 왕복비용 있고 세대수는 거의 안 바뀌는 값이라 danji_code가 있으면 재사용).
    already_matched = {
        row[0]: row[1] for row in conn.execute(
            "SELECT apt_seq, households FROM complexes WHERE households IS NOT NULL"
        )
    }
    resolver = HouseholdsResolver(os.environ["APT_API_KEY"])
    resolved_count = 0
    skipped_apt_seqs = set()

    for apt_seq, group in cleaned.groupby('apt_seq'):
        name = group['complex_name'].iloc[0]
        by = group['built_year'].dropna()
        built_year = int(by.iloc[0]) if not by.empty else None
        dong_name = group['dong_name'].iloc[0]
        lawd_cd = group['lawd_cd'].iloc[0]
        region_info = REGIONS.get(lawd_cd)
        region_code = region_info['dongs'].get(dong_name) if region_info else None
        if region_code is None:
            if region_info is None:
                log.error(f"미등록 LAWD_CD '{lawd_cd}' (apt_seq={apt_seq}) — REGIONS에 없음, 이 단지 통째로 스킵")
                skipped_apt_seqs.add(apt_seq)
                continue
            log.warning(f"미등록 동명 '{dong_name}' (lawd_cd={lawd_cd}, apt_seq={apt_seq}) — {region_info['sigungu']} 구 단위로 대체")
            region_code = region_info['region_code']

        if apt_seq in already_matched:
            households, danji_code, match_confidence = already_matched[apt_seq], None, None
        else:
            households, danji_code, match_confidence = resolver.resolve(region_code, name, built_year)
            if households is not None:
                resolved_count += 1

        conn.execute(
            """INSERT INTO complexes (complex_name, region_code, apt_seq, built_year, households, danji_code, match_confidence)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (apt_seq) DO UPDATE SET
                 complex_name=EXCLUDED.complex_name, region_code=EXCLUDED.region_code,
                 households=COALESCE(complexes.households, EXCLUDED.households),
                 danji_code=COALESCE(complexes.danji_code, EXCLUDED.danji_code),
                 match_confidence=COALESCE(complexes.match_confidence, EXCLUDED.match_confidence)""",
            (name, region_code, apt_seq, built_year, households, danji_code, match_confidence)
        )
    conn.commit()
    log.info(f"세대수 매칭 — 이번에 새로 매칭 {resolved_count}건, 기존 캐시 {len(already_matched)}건")
    if skipped_apt_seqs:
        cleaned = cleaned[~cleaned['apt_seq'].isin(skipped_apt_seqs)].copy()

    complex_id_map = {
        row[1]: row[0] for row in conn.execute("SELECT complex_id, apt_seq FROM complexes")
    }

    # 원본 거래 적재 (raw_source_id로 중복방지) — 원격 DB라 묶어서 insert
    txn_rows = []
    for _, row in cleaned.iterrows():
        floor = int(row['floor']) if pd.notna(row['floor']) else None
        txn_day = int(row['txn_day']) if pd.notna(row['txn_day']) else None
        raw_source_id = f"{row['apt_seq']}_{row['txn_ym']}_{txn_day}_{row['price_10k']}_{floor}"
        txn_rows.append((
            complex_id_map[row['apt_seq']], pg_id, row['area_m2'], floor,
            row['txn_ym'], txn_day, row['price_10k'],
            bool(row['is_direct_txn']), bool(row['is_cancelled']), raw_source_id
        ))
    db.insert_many(
        conn, 'transactions',
        ['complex_id', 'pyeong_group_id', 'area_m2', 'floor', 'txn_ym', 'txn_day',
         'price_10k', 'is_direct_txn', 'is_cancelled', 'raw_source_id'],
        txn_rows,
        on_conflict='ON CONFLICT (raw_source_id) DO NOTHING',
    )
    conn.commit()
    rows_ingested = len(txn_rows)

    # 월별 대표가 재계산 — 이번에 수집된 (단지×월) 건만 갱신.
    monthly = build_monthly_price(cleaned)
    excluded_total = int(monthly['excluded_outlier_count'].sum())
    if excluded_total:
        log.info(f"기준가 이탈 의심 거래 배제 — {excluded_total}건 (증여성 저가거래 등)")
    monthly_rows = [
        (complex_id_map[r['apt_seq']], pg_id, r['ym'], int(r['rep_price_10k']), int(r['txn_count']))
        for _, r in monthly.iterrows()
    ]
    db.insert_many(
        conn, 'monthly_price',
        ['complex_id', 'pyeong_group_id', 'ym', 'rep_price_10k', 'txn_count'],
        monthly_rows,
        on_conflict='''ON CONFLICT (complex_id, pyeong_group_id, ym)
                        DO UPDATE SET rep_price_10k=EXCLUDED.rep_price_10k, txn_count=EXCLUDED.txn_count''',
    )
    conn.commit()
    log.info(f"LOAD 완료 — 거래 {rows_ingested}건, 월별대표가 {len(monthly)}행")
    return rows_ingested


# ------------------------------------------------------------
# RECOMPUTE: 대장단지 판정 + 국면별 상승률 + 동조지수
# ------------------------------------------------------------
def recompute(conn):
    """
    동(region_code) 단위로 각각 대장단지를 뽑고 그 동 안의 단지끼리만 동조판정한다.
    (예전엔 구 안의 동을 다 섞어서 1등 하나만 뽑았음 — 그래서 강남구 기준으로는
    압구정동 단지가 "서울 강남구 대치동" 화면에 대장단지로 뜨는 모순이 생겼음.
    동조판정은 원래 "같은 생활권 내에서 대장 따라가는 단지"를 찾는 거라, 행정동을
    안 나누면 비교 자체가 의미가 없음. REGIONS에 등록된 모든 구×동 조합에 동일 적용.)
    """
    log.info("RECOMPUTE 시작")
    params = AlgoParams()
    pyeong_group_id = conn.execute("SELECT pyeong_group_id FROM pyeong_groups LIMIT 1").fetchone()[0]

    complexes = conn.execute("SELECT complex_id, apt_seq, complex_name, region_code FROM complexes").fetchall()

    # 단지마다 매번 SELECT 왕복하면 원격 DB에서 느림 — 이 평형의 월별가격 전체를 한 번에 긁어서
    # 메모리에서 단지별로 나눔
    all_monthly = pd.DataFrame(
        conn.execute(
            "SELECT complex_id, ym, rep_price_10k, txn_count FROM monthly_price WHERE pyeong_group_id=%s ORDER BY ym",
            (pyeong_group_id,)
        ).fetchall(),
        columns=['complex_id', 'ym', 'rep_price_10k', 'txn_count']
    )
    monthly_by_complex = {
        cid: g[['ym', 'rep_price_10k', 'txn_count']].reset_index(drop=True)
        for cid, g in all_monthly.groupby('complex_id')
    }
    empty_monthly = pd.DataFrame(columns=['ym', 'rep_price_10k', 'txn_count'])

    def load_monthly(complex_id: int) -> pd.DataFrame:
        return monthly_by_complex.get(complex_id, empty_monthly)

    conn.execute("DELETE FROM leader_complexes")
    conn.execute("DELETE FROM regime_returns")
    conn.execute("DELETE FROM sync_summary")

    leader_rows = []
    regime_return_rows = []
    sync_summary_rows = []
    as_of = datetime.now(timezone.utc).date()
    updated_at = datetime.now(timezone.utc)

    regions = sorted({c[3] for c in complexes})
    for region_code in regions:
        region_complexes = [c for c in complexes if c[3] == region_code]
        if len(region_complexes) < 2:
            log.info(f"[{region_code}] 단지 {len(region_complexes)}개뿐 — 대장단지 판정 스킵")
            continue

        region_cids = {c[0] for c in region_complexes}
        region_price_df = (
            all_monthly[all_monthly['complex_id'].isin(region_cids)][['complex_id', 'ym', 'rep_price_10k']]
            .rename(columns={'rep_price_10k': 'price_per_pyeong'})
        )
        ranking = pick_leader(region_price_df, REGIMES, params)
        if ranking.empty:
            log.info(f"[{region_code}] 대장단지 판정 불가 — 국면 종료시점 데이터 없음, 스킵")
            continue

        leader_id = int(ranking.iloc[0]['complex_id'])
        stable_regimes = int(ranking.iloc[0]['stable_regimes'])
        leader_apt_seq, leader_name = next((c[1], c[2]) for c in region_complexes if c[0] == leader_id)
        log.info(
            f"[{region_code}] 대장단지: {leader_name} ({leader_apt_seq}) "
            f"stable_regimes={stable_regimes}/{len(REGIMES)}"
        )
        leader_rows.append((region_code, pyeong_group_id, leader_id, stable_regimes, as_of))

        leader_monthly = load_monthly(leader_id)
        for regime in REGIMES:
            stat = compute_regime_return(leader_monthly, regime, params)
            rr = stat['return_rate']
            regime_return_rows.append((
                leader_id, pyeong_group_id, regime.regime_id,
                float(rr) if rr is not None else None,
                int(stat['txn_count_regime']), bool(stat['is_judgable'])
            ))

        leader_price = int(leader_monthly.iloc[-1]['rep_price_10k'])
        for cid, apt_seq, name, _ in region_complexes:
            if cid == leader_id:
                continue
            cand_monthly = load_monthly(cid)
            if cand_monthly.empty:
                continue
            result = evaluate_candidate(leader_monthly, cand_monthly, REGIMES, params)

            for r in result['per_regime']:
                cr = r['candidate_return']
                regime_return_rows.append((
                    cid, pyeong_group_id, r['regime_id'],
                    float(cr) if cr is not None else None, None, bool(r['judgable'])
                ))

            cand_price = int(cand_monthly.iloc[-1]['rep_price_10k'])
            gap = cand_price - leader_price

            sync_summary_rows.append((
                region_code, pyeong_group_id, leader_id, cid,
                int(result['sync_count']), int(result['judgable_count']),
                float(result['sync_index']), gap, updated_at
            ))

    db.insert_many(
        conn, 'leader_complexes',
        ['region_code', 'pyeong_group_id', 'complex_id', 'stable_regimes', 'as_of'],
        leader_rows,
    )
    db.insert_many(
        conn, 'regime_returns',
        ['complex_id', 'pyeong_group_id', 'regime_id', 'return_rate', 'txn_count_regime', 'is_judgable'],
        regime_return_rows,
    )
    db.insert_many(
        conn, 'sync_summary',
        ['region_code', 'pyeong_group_id', 'leader_complex_id', 'candidate_complex_id',
         'sync_count', 'judgable_count', 'sync_index', 'current_price_gap_10k', 'updated_at'],
        sync_summary_rows,
    )
    conn.commit()
    log.info(f"RECOMPUTE 완료 — {len(leader_rows)}개 지역 대장단지 판정")


# ------------------------------------------------------------
# 메인 엔트리포인트 (cron/Airflow가 호출하는 지점)
# ------------------------------------------------------------
def run(mode: str) -> dict:
    started = datetime.now(timezone.utc)
    conn = db.get_conn()

    run_id = None
    try:
        init_db(conn)
        run_id = conn.execute(
            "INSERT INTO pipeline_runs (started_at, status) VALUES (%s, 'running') RETURNING run_id",
            (started,)
        ).fetchone()[0]
        conn.commit()

        raw = extract(mode)
        rows_ingested = transform_and_load(conn, raw)
        recompute(conn)

        conn.execute(
            "UPDATE pipeline_runs SET finished_at=%s, status='success', rows_ingested=%s WHERE run_id=%s",
            (datetime.now(timezone.utc), rows_ingested, run_id)
        )
        conn.commit()
        log.info("파이프라인 성공")
        return {'status': 'success', 'run_id': run_id}

    except Exception as e:
        log.exception("파이프라인 실패")
        # 실패 지점에 따라 트랜잭션이 abort 상태로 남아있을 수 있음(예: FK 위반) —
        # rollback 없이 바로 UPDATE하면 "current transaction is aborted"로 또 실패해서
        # 진짜 원인이 로그에서 묻힘.
        conn.rollback()
        if run_id is not None:
            conn.execute(
                "UPDATE pipeline_runs SET finished_at=%s, status='failed', error_msg=%s WHERE run_id=%s",
                (datetime.now(timezone.utc), str(e), run_id)
            )
            conn.commit()
        return {'status': 'failed', 'error': str(e)}
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['full', 'incremental'], default='incremental')
    args = parser.parse_args()

    result = run(args.mode)
    sys.exit(0 if result['status'] == 'success' else 1)
