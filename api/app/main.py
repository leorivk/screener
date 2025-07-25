from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
import clickhouse_connect

app = FastAPI()

def get_db_client():
    # Ensure you are using the correct password as set in docker-compose.yml
    client = clickhouse_connect.get_client(host='localhost', port=8123, user='default', password='password123')
    try:
        yield client
    finally:
        client.close()

class ScreenerRequest(BaseModel):
    market: Optional[str] = None
    sector: Optional[str] = None
    min_market_cap: Optional[int] = Field(None, alias="minMarketCap")
    max_per: Optional[float] = Field(None, alias="maxPer")

@app.post("/screener")
def screen_stocks(request: ScreenerRequest, client: clickhouse_connect.driver.Client = Depends(get_db_client)):
    query = "SELECT * FROM stocks"
    params = {}
    where_clauses = []

    if request.market:
        where_clauses.append("market = %(market)s")
        params['market'] = request.market
    if request.sector:
        where_clauses.append("sector = %(sector)s")
        params['sector'] = request.sector
    if request.min_market_cap is not None:
        where_clauses.append("marketCap >= %(min_market_cap)s")
        params['min_market_cap'] = request.min_market_cap
    if request.max_per is not None:
        where_clauses.append("per <= %(max_per)s")
        params['max_per'] = request.max_per
    
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    try:
        # Execute the query and get raw results
        result = client.query(query, parameters=params)
        
        # Manually convert the raw result to a list of dictionaries
        column_names = result.column_names
        data = [dict(zip(column_names, row)) for row in result.result_rows]
        
        return data
    except Exception as e:
        # Catch any exception during query execution and return a detailed error
        print(f"An error occurred during query execution: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


# /update-data 엔드포인트는 제거됨. 데이터 업데이트는 ETL 파이프라인의 책임.
