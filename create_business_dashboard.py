#!/usr/bin/env python3
"""
실시간 비즈니스 대시보드 생성 스크립트
- 실시간 매출 모니터링
- 고객 행동 분석
- 브랜드별 성과 트래킹
"""

import requests
import json
import time

KIBANA_URL = "http://localhost:5601"
KIBANA_API = f"{KIBANA_URL}/api"

def wait_for_kibana():
    """Kibana가 준비될 때까지 대기"""
    print("🔄 Kibana 준비 상태 확인 중...")
    for i in range(30):
        try:
            response = requests.get(f"{KIBANA_URL}/api/status", timeout=5)
            if response.status_code == 200:
                print("✅ Kibana 준비 완료!")
                return True
        except:
            pass
        time.sleep(2)
    return False

def create_orders_index_pattern():
    """주문 데이터 인덱스 패턴 생성"""
    print("📋 주문 데이터 인덱스 패턴 생성 중...")

    index_pattern = {
        "attributes": {
            "title": "orders-*",
            "timeFieldName": "@timestamp"
        }
    }

    try:
        response = requests.post(
            f"{KIBANA_API}/saved_objects/index-pattern/orders-pattern",
            headers={"Content-Type": "application/json", "kbn-xsrf": "true"},
            json=index_pattern
        )

        if response.status_code in [200, 409]:
            print("✅ Orders 인덱스 패턴 생성 완료")
            return True
        else:
            print(f"❌ 인덱스 패턴 생성 실패: {response.text}")
            return False
    except Exception as e:
        print(f"❌ 인덱스 패턴 생성 오류: {e}")
        return False

def create_business_visualizations():
    """비즈니스 시각화 생성"""
    print("📊 비즈니스 시각화 생성 중...")

    visualizations = []

    # 1. 실시간 매출 트렌드 (라인 차트)
    revenue_trend = {
        "attributes": {
            "title": "실시간 매출 트렌드",
            "type": "line",
            "visState": json.dumps({
                "title": "실시간 매출 트렌드",
                "type": "line",
                "aggs": [
                    {
                        "id": "1",
                        "type": "sum",
                        "schema": "metric",
                        "params": {
                            "field": "total_amount"
                        }
                    },
                    {
                        "id": "2",
                        "type": "date_histogram",
                        "schema": "segment",
                        "params": {
                            "field": "order_date",
                            "interval": "1h",
                            "min_doc_count": 1
                        }
                    }
                ],
                "params": {
                    "grid": {"categoryLines": False, "style": {"color": "#eee"}},
                    "categoryAxes": [{
                        "id": "CategoryAxis-1",
                        "type": "category",
                        "position": "bottom",
                        "show": True,
                        "title": {"text": "시간"}
                    }],
                    "valueAxes": [{
                        "id": "ValueAxis-1",
                        "name": "LeftAxis-1",
                        "type": "value",
                        "position": "left",
                        "show": True,
                        "title": {"text": "매출 (원)"}
                    }],
                    "seriesParams": [{
                        "show": True,
                        "type": "line",
                        "mode": "normal",
                        "data": {"label": "매출", "id": "1"},
                        "valueAxis": "ValueAxis-1",
                        "drawLinesBetweenPoints": True,
                        "showCircles": True
                    }],
                    "addTooltip": True,
                    "addLegend": True,
                    "legendPosition": "right",
                    "times": [],
                    "addTimeMarker": False
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "orders-pattern",
                    "query": {
                        "match_all": {}
                    },
                    "filter": []
                })
            }
        }
    }

    # 2. 브랜드별 매출 순위 (막대 차트)
    brand_performance = {
        "attributes": {
            "title": "브랜드별 매출 성과",
            "type": "histogram",
            "visState": json.dumps({
                "title": "브랜드별 매출 성과",
                "type": "histogram",
                "aggs": [
                    {
                        "id": "1",
                        "type": "sum",
                        "schema": "metric",
                        "params": {
                            "field": "total_amount"
                        }
                    },
                    {
                        "id": "2",
                        "type": "terms",
                        "schema": "segment",
                        "params": {
                            "field": "brands.keyword",
                            "orderBy": "1",
                            "order": "desc",
                            "size": 10
                        }
                    }
                ],
                "params": {
                    "grid": {"categoryLines": False, "style": {"color": "#eee"}},
                    "categoryAxes": [{
                        "id": "CategoryAxis-1",
                        "type": "category",
                        "position": "bottom",
                        "show": True,
                        "title": {"text": "브랜드"}
                    }],
                    "valueAxes": [{
                        "id": "ValueAxis-1",
                        "name": "LeftAxis-1",
                        "type": "value",
                        "position": "left",
                        "show": True,
                        "title": {"text": "매출 (원)"}
                    }],
                    "seriesParams": [{
                        "show": True,
                        "type": "histogram",
                        "mode": "stacked",
                        "data": {"label": "매출", "id": "1"},
                        "valueAxis": "ValueAxis-1"
                    }],
                    "addTooltip": True,
                    "addLegend": True,
                    "legendPosition": "right",
                    "times": [],
                    "addTimeMarker": False
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "orders-pattern",
                    "query": {
                        "match_all": {}
                    },
                    "filter": []
                })
            }
        }
    }

    # 3. 고객 행동 분석 - 결제 방법별 분포 (파이 차트)
    payment_behavior = {
        "attributes": {
            "title": "결제 방법별 고객 행동",
            "type": "pie",
            "visState": json.dumps({
                "title": "결제 방법별 고객 행동",
                "type": "pie",
                "aggs": [
                    {
                        "id": "1",
                        "type": "count",
                        "schema": "metric",
                        "params": {}
                    },
                    {
                        "id": "2",
                        "type": "terms",
                        "schema": "segment",
                        "params": {
                            "field": "payment_method.keyword",
                            "orderBy": "1",
                            "order": "desc",
                            "size": 5
                        }
                    }
                ],
                "params": {
                    "addTooltip": True,
                    "addLegend": True,
                    "legendPosition": "right",
                    "isDonut": False
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "orders-pattern",
                    "query": {
                        "match_all": {}
                    },
                    "filter": []
                })
            }
        }
    }

    # 4. 주문 상태별 분석 (도넛 차트)
    order_status_analysis = {
        "attributes": {
            "title": "주문 상태별 분석",
            "type": "pie",
            "visState": json.dumps({
                "title": "주문 상태별 분석",
                "type": "pie",
                "aggs": [
                    {
                        "id": "1",
                        "type": "count",
                        "schema": "metric",
                        "params": {}
                    },
                    {
                        "id": "2",
                        "type": "terms",
                        "schema": "segment",
                        "params": {
                            "field": "status.keyword",
                            "orderBy": "1",
                            "order": "desc",
                            "size": 6
                        }
                    }
                ],
                "params": {
                    "addTooltip": True,
                    "addLegend": True,
                    "legendPosition": "right",
                    "isDonut": True
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "orders-pattern",
                    "query": {
                        "match_all": {}
                    },
                    "filter": []
                })
            }
        }
    }

    # 5. 카테고리별 판매량 (가로 막대 차트)
    category_sales = {
        "attributes": {
            "title": "카테고리별 판매량",
            "type": "horizontal_bar",
            "visState": json.dumps({
                "title": "카테고리별 판매량",
                "type": "horizontal_bar",
                "aggs": [
                    {
                        "id": "1",
                        "type": "sum",
                        "schema": "metric",
                        "params": {
                            "field": "total_quantity"
                        }
                    },
                    {
                        "id": "2",
                        "type": "terms",
                        "schema": "segment",
                        "params": {
                            "field": "categories.keyword",
                            "orderBy": "1",
                            "order": "desc",
                            "size": 8
                        }
                    }
                ],
                "params": {
                    "grid": {"categoryLines": False, "style": {"color": "#eee"}},
                    "categoryAxes": [{
                        "id": "CategoryAxis-1",
                        "type": "category",
                        "position": "left",
                        "show": True,
                        "title": {"text": "카테고리"}
                    }],
                    "valueAxes": [{
                        "id": "ValueAxis-1",
                        "name": "BottomAxis-1",
                        "type": "value",
                        "position": "bottom",
                        "show": True,
                        "title": {"text": "판매량"}
                    }],
                    "seriesParams": [{
                        "show": True,
                        "type": "histogram",
                        "mode": "stacked",
                        "data": {"label": "판매량", "id": "1"},
                        "valueAxis": "ValueAxis-1"
                    }],
                    "addTooltip": True,
                    "addLegend": True,
                    "legendPosition": "right"
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "orders-pattern",
                    "query": {
                        "match_all": {}
                    },
                    "filter": []
                })
            }
        }
    }

    # 6. 실시간 매출 게이지
    revenue_gauge = {
        "attributes": {
            "title": "실시간 총 매출",
            "type": "gauge",
            "visState": json.dumps({
                "title": "실시간 총 매출",
                "type": "gauge",
                "aggs": [
                    {
                        "id": "1",
                        "type": "sum",
                        "schema": "metric",
                        "params": {
                            "field": "total_amount"
                        }
                    }
                ],
                "params": {
                    "addTooltip": True,
                    "addLegend": False,
                    "type": "gauge",
                    "gauge": {
                        "alignment": "automatic",
                        "extendRange": True,
                        "percentageMode": False,
                        "gaugeType": "Arc",
                        "gaugeStyle": "Full",
                        "backStyle": "Full",
                        "orientation": "vertical",
                        "colorSchema": "Green to Red",
                        "gaugeColorMode": "Labels",
                        "colorsRange": [
                            {"from": 0, "to": 50000000},
                            {"from": 50000000, "to": 100000000},
                            {"from": 100000000, "to": 200000000}
                        ],
                        "invertColors": False,
                        "labels": {
                            "show": True,
                            "color": "black"
                        },
                        "scale": {
                            "show": True,
                            "labels": False,
                            "color": "#333"
                        },
                        "type": "meter",
                        "style": {
                            "bgFill": "#eee",
                            "bgColor": False,
                            "labelColor": False,
                            "subText": "",
                            "fontSize": 60
                        }
                    }
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "orders-pattern",
                    "query": {
                        "range": {
                            "order_date": {
                                "gte": "now-24h",
                                "lte": "now"
                            }
                        }
                    },
                    "filter": []
                })
            }
        }
    }

    visualizations = [
        ("revenue-trend", revenue_trend),
        ("brand-performance", brand_performance),
        ("payment-behavior", payment_behavior),
        ("order-status-analysis", order_status_analysis),
        ("category-sales", category_sales),
        ("revenue-gauge", revenue_gauge)
    ]

    created_vis = []
    for vis_id, vis_config in visualizations:
        try:
            response = requests.post(
                f"{KIBANA_API}/saved_objects/visualization/{vis_id}",
                headers={"Content-Type": "application/json", "kbn-xsrf": "true"},
                json=vis_config
            )

            if response.status_code in [200, 409]:
                print(f"✅ '{vis_config['attributes']['title']}' 시각화 생성 완료")
                created_vis.append(vis_id)
            else:
                print(f"❌ '{vis_config['attributes']['title']}' 생성 실패: {response.text}")
        except Exception as e:
            print(f"❌ 시각화 생성 오류: {e}")

    return created_vis

def create_business_dashboard(visualization_ids):
    """실시간 비즈니스 대시보드 생성"""
    print("🎯 실시간 비즈니스 대시보드 생성 중...")

    # 패널 레이아웃 정의 (6개 시각화를 2x3 그리드로 배치)
    panels = []
    panel_configs = [
        {"id": "revenue-gauge", "title": "실시간 총 매출", "x": 0, "y": 0, "w": 24, "h": 15},
        {"id": "revenue-trend", "title": "매출 트렌드", "x": 24, "y": 0, "w": 24, "h": 15},
        {"id": "brand-performance", "title": "브랜드 성과", "x": 0, "y": 15, "w": 24, "h": 15},
        {"id": "category-sales", "title": "카테고리 판매량", "x": 24, "y": 15, "w": 24, "h": 15},
        {"id": "payment-behavior", "title": "결제 방법 분석", "x": 0, "y": 30, "w": 24, "h": 15},
        {"id": "order-status-analysis", "title": "주문 상태 분석", "x": 24, "y": 30, "w": 24, "h": 15}
    ]

    for i, config in enumerate(panel_configs):
        if config["id"] in visualization_ids:
            panel = {
                "version": "8.8.0",
                "gridData": {
                    "x": config["x"],
                    "y": config["y"],
                    "w": config["w"],
                    "h": config["h"],
                    "i": str(i)
                },
                "panelIndex": str(i),
                "embeddableConfig": {},
                "panelRefName": f"panel_{i}"
            }
            panels.append(panel)

    dashboard = {
        "attributes": {
            "title": "🔥 실시간 비즈니스 대시보드",
            "type": "dashboard",
            "description": "실시간 매출 모니터링, 고객 행동 분석, 브랜드 성과 트래킹",
            "panelsJSON": json.dumps(panels),
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-24h",
            "refreshInterval": {
                "pause": False,
                "value": 30000  # 30초마다 자동 새로고침
            },
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {
                        "match_all": {}
                    },
                    "filter": []
                })
            }
        },
        "references": []
    }

    # 참조 추가
    for i, config in enumerate(panel_configs):
        if config["id"] in visualization_ids:
            dashboard["references"].append({
                "name": f"panel_{i}",
                "type": "visualization",
                "id": config["id"]
            })

    try:
        response = requests.post(
            f"{KIBANA_API}/saved_objects/dashboard/realtime-business-dashboard",
            headers={"Content-Type": "application/json", "kbn-xsrf": "true"},
            json=dashboard
        )

        if response.status_code in [200, 409]:
            print("🎉 실시간 비즈니스 대시보드 생성 완료!")
            print(f"🌐 대시보드 URL: {KIBANA_URL}/app/dashboards#/view/realtime-business-dashboard")
            return True
        else:
            print(f"❌ 대시보드 생성 실패: {response.text}")
            return False
    except Exception as e:
        print(f"❌ 대시보드 생성 오류: {e}")
        return False

def main():
    """메인 함수"""
    print("🚀 실시간 비즈니스 대시보드 생성 시작!")
    print("=" * 60)

    # 1. Kibana 준비 대기
    if not wait_for_kibana():
        print("❌ Kibana가 준비되지 않았습니다. Kibana 실행 상태를 확인해주세요.")
        return

    # 2. Orders 인덱스 패턴 생성
    if not create_orders_index_pattern():
        print("❌ Orders 인덱스 패턴 생성 실패")
        return

    # 3. 비즈니스 시각화 생성
    vis_ids = create_business_visualizations()
    if not vis_ids:
        print("❌ 시각화 생성 실패")
        return

    # 4. 비즈니스 대시보드 생성
    if create_business_dashboard(vis_ids):
        print("=" * 60)
        print("🎉 실시간 비즈니스 대시보드 구축 완료!")
        print("")
        print("📊 접속 방법:")
        print(f"   1. 브라우저에서 {KIBANA_URL} 접속")
        print("   2. 왼쪽 메뉴에서 'Dashboard' 클릭")
        print("   3. '🔥 실시간 비즈니스 대시보드' 선택")
        print("")
        print("🔄 대시보드는 30초마다 자동으로 새로고침됩니다")
        print("⏰ 기본 시간 범위: 최근 24시간")
        print("")
        print("📈 포함된 분석:")
        print("   - 실시간 총 매출 게이지")
        print("   - 시간별 매출 트렌드")
        print("   - 브랜드별 매출 성과")
        print("   - 카테고리별 판매량")
        print("   - 결제 방법별 고객 행동 분석")
        print("   - 주문 상태별 분석")
    else:
        print("❌ 대시보드 생성 실패")

if __name__ == "__main__":
    main()