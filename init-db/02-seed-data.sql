-- Seed Data for E-commerce Database
-- 전자상거래 데이터베이스 시드 데이터

-- 카테고리 데이터 삽입
INSERT INTO categories (name, description) VALUES
('Electronics', '전자제품 및 IT 기기'),
('Clothing', '의류 및 패션 아이템'),
('Books', '도서 및 교육 자료'),
('Home & Garden', '홈인테리어 및 원예용품'),
('Sports', '스포츠 및 아웃도어 용품'),
('Beauty', '화장품 및 뷰티 제품'),
('Toys', '장난감 및 게임'),
('Automotive', '자동차 용품 및 부품'),
('Food', '식품 및 음료'),
('Health', '건강 및 의료용품');

-- 브랜드 데이터 삽입
INSERT INTO brands (name, description) VALUES
('Samsung', '글로벌 전자제품 브랜드'),
('Apple', '혁신적인 기술 제품'),
('Nike', '스포츠웨어 및 운동화'),
('Adidas', '스포츠 의류 및 신발'),
('Sony', '전자제품 및 엔터테인먼트'),
('LG', '생활가전 및 전자제품'),
('Zara', '패스트 패션 브랜드'),
('H&M', '저렴한 패션 의류'),
('Canon', '카메라 및 프린터'),
('Dell', '컴퓨터 및 IT 장비'),
('HP', '컴퓨터 및 프린터'),
('Lenovo', '노트북 및 컴퓨터');

-- 뷰 생성: 상품 통계
CREATE VIEW product_stats AS
SELECT
    p.product_id,
    p.name,
    c.name as category_name,
    b.name as brand_name,
    p.price,
    p.rating,
    p.rating_count,
    p.stock_quantity,
    COALESCE(order_stats.total_sold, 0) as total_sold,
    COALESCE(order_stats.total_revenue, 0) as total_revenue
FROM products p
LEFT JOIN categories c ON p.category_id = c.category_id
LEFT JOIN brands b ON p.brand_id = b.brand_id
LEFT JOIN (
    SELECT
        oi.product_id,
        SUM(oi.quantity) as total_sold,
        SUM(oi.total_price) as total_revenue
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY oi.product_id
) order_stats ON p.product_id = order_stats.product_id;

-- 뷰 생성: 사용자 통계
CREATE VIEW user_stats AS
SELECT
    u.user_id,
    u.name,
    u.email,
    u.signup_date,
    COALESCE(order_stats.total_orders, 0) as total_orders,
    COALESCE(order_stats.total_spent, 0) as total_spent,
    COALESCE(order_stats.avg_order_value, 0) as avg_order_value,
    COALESCE(behavior_stats.total_views, 0) as total_views,
    COALESCE(behavior_stats.total_searches, 0) as total_searches
FROM users u
LEFT JOIN (
    SELECT
        o.user_id,
        COUNT(*) as total_orders,
        SUM(o.total_amount) as total_spent,
        AVG(o.total_amount) as avg_order_value
    FROM orders o
    WHERE o.status IN ('shipped', 'delivered')
    GROUP BY o.user_id
) order_stats ON u.user_id = order_stats.user_id
LEFT JOIN (
    SELECT
        ubl.user_id,
        SUM(CASE WHEN ubl.action_type = 'view' THEN 1 ELSE 0 END) as total_views,
        SUM(CASE WHEN ubl.action_type = 'search' THEN 1 ELSE 0 END) as total_searches
    FROM user_behavior_log ubl
    GROUP BY ubl.user_id
) behavior_stats ON u.user_id = behavior_stats.user_id;

-- 함수: 상품 추천 점수 계산
CREATE OR REPLACE FUNCTION calculate_product_recommendation_score(
    p_product_id VARCHAR(20),
    p_user_id VARCHAR(20) DEFAULT NULL
)
RETURNS DECIMAL(10,4) AS $$
DECLARE
    rating_score DECIMAL(10,4) := 0;
    popularity_score DECIMAL(10,4) := 0;
    user_preference_score DECIMAL(10,4) := 0;
    final_score DECIMAL(10,4) := 0;
BEGIN
    -- 평점 점수 (40%)
    SELECT COALESCE(rating * rating_count / 100.0, 0) INTO rating_score
    FROM products
    WHERE product_id = p_product_id;

    -- 인기도 점수 (40%)
    SELECT COALESCE(LOG(total_sold + 1) * 2, 0) INTO popularity_score
    FROM product_stats
    WHERE product_id = p_product_id;

    -- 사용자 선호도 점수 (20%) - 카테고리 기반
    IF p_user_id IS NOT NULL THEN
        SELECT COALESCE(AVG(ps.total_sold), 0) / 10.0 INTO user_preference_score
        FROM product_stats ps
        JOIN products p ON ps.product_id = p.product_id
        WHERE p.category_id IN (
            SELECT DISTINCT p2.category_id
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products p2 ON oi.product_id = p2.product_id
            WHERE o.user_id = p_user_id
        );
    END IF;

    final_score := (rating_score * 0.4) + (popularity_score * 0.4) + (user_preference_score * 0.2);

    RETURN GREATEST(final_score, 0);
END;
$$ LANGUAGE plpgsql;

-- 분석용 집계 테이블 생성
CREATE TABLE daily_sales_summary (
    summary_date DATE PRIMARY KEY,
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0,
    unique_customers INTEGER DEFAULT 0,
    avg_order_value DECIMAL(10,2) DEFAULT 0,
    top_category VARCHAR(50),
    top_product VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 매일 판매 요약 업데이트 함수
CREATE OR REPLACE FUNCTION update_daily_sales_summary(summary_date DATE)
RETURNS VOID AS $$
BEGIN
    INSERT INTO daily_sales_summary (
        summary_date,
        total_orders,
        total_revenue,
        unique_customers,
        avg_order_value,
        top_category,
        top_product
    )
    SELECT
        summary_date,
        COUNT(*) as total_orders,
        SUM(total_amount) as total_revenue,
        COUNT(DISTINCT user_id) as unique_customers,
        AVG(total_amount) as avg_order_value,
        (SELECT c.name
         FROM categories c
         JOIN products p ON c.category_id = p.category_id
         JOIN order_items oi ON p.product_id = oi.product_id
         JOIN orders o2 ON oi.order_id = o2.order_id
         WHERE DATE(o2.order_date) = summary_date
         GROUP BY c.name
         ORDER BY SUM(oi.quantity) DESC
         LIMIT 1) as top_category,
        (SELECT oi.product_id
         FROM order_items oi
         JOIN orders o2 ON oi.order_id = o2.order_id
         WHERE DATE(o2.order_date) = summary_date
         GROUP BY oi.product_id
         ORDER BY SUM(oi.quantity) DESC
         LIMIT 1) as top_product
    FROM orders o
    WHERE DATE(o.order_date) = summary_date
    AND o.status IN ('shipped', 'delivered')
    ON CONFLICT (summary_date)
    DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        unique_customers = EXCLUDED.unique_customers,
        avg_order_value = EXCLUDED.avg_order_value,
        top_category = EXCLUDED.top_category,
        top_product = EXCLUDED.top_product;
END;
$$ LANGUAGE plpgsql;