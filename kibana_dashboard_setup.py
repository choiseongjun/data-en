#!/usr/bin/env python3
"""
Kibana SQL Performance Dashboard 자동 구축 스크립트
"""

import requests
import json
import time

KIBANA_URL = "http://localhost:5601"
KIBANA_API = f"{KIBANA_URL}/api"

def wait_for_kibana():
    """Kibana가 준비될 때까지 대기"""
    print("Waiting for Kibana to be ready...")
    for i in range(30):
        try:
            response = requests.get(f"{KIBANA_URL}/api/status", timeout=5)
            if response.status_code == 200:
                print("✅ Kibana is ready!")
                return True
        except:
            pass
        time.sleep(2)
    return False

def create_index_pattern():
    """Index Pattern 생성"""
    print("Creating index pattern for sql-logs...")

    index_pattern = {
        "attributes": {
            "title": "sql-logs-*",
            "timeFieldName": "@timestamp"
        }
    }

    try:
        response = requests.post(
            f"{KIBANA_API}/saved_objects/index-pattern/sql-logs-pattern",
            headers={"Content-Type": "application/json", "kbn-xsrf": "true"},
            json=index_pattern
        )

        if response.status_code in [200, 409]:  # 409 = already exists
            print("✅ Index pattern created/exists")
            return True
        else:
            print(f"❌ Failed to create index pattern: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error creating index pattern: {e}")
        return False

def create_visualizations():
    """Visualization들 생성"""
    visualizations = []

    # 1. SQL 실행시간 분포 (파이 차트)
    performance_pie = {
        "attributes": {
            "title": "SQL Performance Distribution",
            "type": "pie",
            "visState": json.dumps({
                "title": "SQL Performance Distribution",
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
                            "field": "performance_category.keyword",
                            "orderBy": "1",
                            "order": "desc",
                            "size": 5
                        }
                    }
                ],
                "params": {
                    "addTooltip": True,
                    "addLegend": True,
                    "legendPosition": "right"
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "sql-logs-pattern",
                    "query": {
                        "match_all": {}
                    }
                })
            }
        }
    }

    # 2. 시간별 SQL 실행시간 트렌드 (라인 차트)
    time_trend = {
        "attributes": {
            "title": "SQL Execution Time Trend",
            "type": "line",
            "visState": json.dumps({
                "title": "SQL Execution Time Trend",
                "type": "line",
                "aggs": [
                    {
                        "id": "1",
                        "type": "avg",
                        "schema": "metric",
                        "params": {
                            "field": "execution_time_ms"
                        }
                    },
                    {
                        "id": "2",
                        "type": "date_histogram",
                        "schema": "segment",
                        "params": {
                            "field": "@timestamp",
                            "interval": "auto"
                        }
                    }
                ],
                "params": {
                    "grid": {"categoryLines": False, "style": {"color": "#eee"}},
                    "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom", "show": True}],
                    "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "left", "show": True}]
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "sql-logs-pattern",
                    "query": {
                        "bool": {
                            "must": [
                                {"exists": {"field": "execution_time_ms"}}
                            ]
                        }
                    }
                })
            }
        }
    }

    # 3. 쿼리 타입별 통계 (막대 차트)
    query_type_stats = {
        "attributes": {
            "title": "Query Type Statistics",
            "type": "histogram",
            "visState": json.dumps({
                "title": "Query Type Statistics",
                "type": "histogram",
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
                            "field": "query_type.keyword",
                            "orderBy": "1",
                            "order": "desc",
                            "size": 10
                        }
                    }
                ],
                "params": {
                    "grid": {"categoryLines": False, "style": {"color": "#eee"}},
                    "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom", "show": True}],
                    "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "left", "show": True}]
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "sql-logs-pattern",
                    "query": {
                        "bool": {
                            "must": [
                                {"exists": {"field": "query_type"}}
                            ]
                        }
                    }
                })
            }
        }
    }

    # 4. 가장 느린 쿼리들 (테이블)
    slow_queries = {
        "attributes": {
            "title": "Slowest Queries",
            "type": "table",
            "visState": json.dumps({
                "title": "Slowest Queries",
                "type": "table",
                "aggs": [
                    {
                        "id": "1",
                        "type": "max",
                        "schema": "metric",
                        "params": {
                            "field": "execution_time_ms"
                        }
                    },
                    {
                        "id": "2",
                        "type": "terms",
                        "schema": "bucket",
                        "params": {
                            "field": "sql_query.keyword",
                            "orderBy": "1",
                            "order": "desc",
                            "size": 10
                        }
                    }
                ],
                "params": {
                    "perPage": 10,
                    "showPartialRows": False,
                    "showMetricsAtAllLevels": False,
                    "sort": {
                        "columnIndex": None,
                        "direction": None
                    },
                    "showTotal": False,
                    "totalFunc": "sum"
                }
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": "sql-logs-pattern",
                    "query": {
                        "bool": {
                            "must": [
                                {"exists": {"field": "execution_time_ms"}},
                                {"range": {"execution_time_ms": {"gte": 0}}}
                            ]
                        }
                    }
                })
            }
        }
    }

    visualizations = [
        ("sql-performance-pie", performance_pie),
        ("sql-time-trend", time_trend),
        ("query-type-stats", query_type_stats),
        ("slowest-queries", slow_queries)
    ]

    print("Creating visualizations...")
    created_vis = []

    for vis_id, vis_config in visualizations:
        try:
            response = requests.post(
                f"{KIBANA_API}/saved_objects/visualization/{vis_id}",
                headers={"Content-Type": "application/json", "kbn-xsrf": "true"},
                json=vis_config
            )

            if response.status_code in [200, 409]:
                print(f"✅ Visualization '{vis_config['attributes']['title']}' created")
                created_vis.append(vis_id)
            else:
                print(f"❌ Failed to create visualization '{vis_config['attributes']['title']}': {response.text}")
        except Exception as e:
            print(f"❌ Error creating visualization: {e}")

    return created_vis

def create_dashboard(visualization_ids):
    """Dashboard 생성"""
    print("Creating SQL Performance Dashboard...")

    # 패널 레이아웃 정의
    panels = []
    panel_configs = [
        {"id": "sql-performance-pie", "title": "Performance Distribution", "x": 0, "y": 0, "w": 24, "h": 15},
        {"id": "sql-time-trend", "title": "Execution Time Trend", "x": 24, "y": 0, "w": 24, "h": 15},
        {"id": "query-type-stats", "title": "Query Type Stats", "x": 0, "y": 15, "w": 24, "h": 15},
        {"id": "slowest-queries", "title": "Slowest Queries", "x": 24, "y": 15, "w": 24, "h": 15}
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
            "title": "SQL Performance Dashboard",
            "type": "dashboard",
            "description": "Real-time SQL query performance monitoring",
            "panelsJSON": json.dumps(panels),
            "timeRestore": False,
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
            f"{KIBANA_API}/saved_objects/dashboard/sql-performance-dashboard",
            headers={"Content-Type": "application/json", "kbn-xsrf": "true"},
            json=dashboard
        )

        if response.status_code in [200, 409]:
            print("✅ SQL Performance Dashboard created!")
            print(f"🌐 Dashboard URL: {KIBANA_URL}/app/dashboards#/view/sql-performance-dashboard")
            return True
        else:
            print(f"❌ Failed to create dashboard: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error creating dashboard: {e}")
        return False

def main():
    """메인 함수"""
    print("🚀 Setting up Kibana SQL Performance Dashboard...")

    # 1. Kibana 준비 대기
    if not wait_for_kibana():
        print("❌ Kibana is not ready. Please check if Kibana is running.")
        return

    # 2. Index Pattern 생성
    if not create_index_pattern():
        print("❌ Failed to create index pattern")
        return

    # 3. Visualizations 생성
    vis_ids = create_visualizations()
    if not vis_ids:
        print("❌ No visualizations were created")
        return

    # 4. Dashboard 생성
    if create_dashboard(vis_ids):
        print("🎉 SQL Performance Dashboard setup completed!")
        print(f"📊 Access your dashboard at: {KIBANA_URL}/app/dashboards")
    else:
        print("❌ Dashboard creation failed")

if __name__ == "__main__":
    main()