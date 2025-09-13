from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import json
import geopandas as gpd
from shapely.geometry import box
from typing import Optional
import uvicorn
import os
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd


app = FastAPI(title="GeoJSON Streaming API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

geojson_parts = {}

INPUT_DIR = ""
FILENAME_PATTERN = "indore_roads_density_grid_part_{}.geojson"
NUM_PARTS = 3

def load_geojson_parts():
    global geojson_parts
    for i in range(1, NUM_PARTS + 1):
        path = os.path.join(INPUT_DIR, FILENAME_PATTERN.format(i))
        try:
            gdf = gpd.read_file(path)
            geojson_parts[i] = gdf
            print(f"Loaded part {i} with {len(gdf)} features from {path}")
        except Exception as e:
            print(f"Error loading {path}: {e}")
            geojson_parts[i] = None

@app.on_event("startup")
async def startup_event():
    load_geojson_parts()

@app.get("/api/geojson")
async def get_geojson_by_bounds(
    north: float = Query(..., description="North boundary (max latitude)"),
    south: float = Query(..., description="South boundary (min latitude)"),
    east: float = Query(..., description="East boundary (max longitude)"),
    west: float = Query(..., description="West boundary (min longitude)"),
    zoom: int = Query(10, description="Current zoom level"),
):
    """
    Get GeoJSON features within the specified bounding box from all parts combined.
    """
    try:
        bbox = box(west, south, east, north)
        
        # Collect filtered features from all parts
        filtered_parts = []
        total_features = 0

        for i in range(1, NUM_PARTS + 1):
            gdf = geojson_parts.get(i)
            if gdf is None or gdf.empty:
                continue

            filtered_gdf = gdf[gdf.geometry.intersects(bbox)]
            # Apply zoom-based sampling optionally
            if zoom < 10:
                filtered_gdf = filtered_gdf.iloc[::max(1, len(filtered_gdf) // 100)]
            elif zoom < 13:
                filtered_gdf = filtered_gdf.iloc[::max(1, len(filtered_gdf) // 500)]

            filtered_parts.append(filtered_gdf)

        if filtered_parts:
            combined_gdf = gpd.GeoDataFrame(pd.concat(filtered_parts, ignore_index=True), crs=filtered_parts[0].crs)
            total_features = len(combined_gdf)
            geojson = json.loads(combined_gdf.to_json())
        else:
            total_features = 0
            geojson = {"type": "FeatureCollection", "features": []}

        return {
            "type": "FeatureCollection",
            "features": geojson["features"],
            "count": total_features,
            "bounds": {
                "north": north,
                "south": south,
                "east": east,
                "west": west
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

@app.get("/api/info")
async def get_info():
    """Return cumulative info for all parts combined."""
    try:
        info = {
            "total_features": 0,
            "bounds": None,
            "columns": set()
        }
        gdfs = [g for g in geojson_parts.values() if g is not None and not g.empty]
        if not gdfs:
            return {"status": "No data loaded"}

        # Concatenate all parts to get combined bounds, columns, and total features
        combined = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
        bounds = combined.total_bounds
        info["total_features"] = len(combined)
        info["bounds"] = {
            "west": float(bounds[0]),
            "south": float(bounds[1]),
            "east": float(bounds[2]),
            "north": float(bounds[3])
        }
        info["columns"] = list(combined.columns)
        return info
    except Exception as e:
        return {"status": f"Error retrieving info: {str(e)}"}

@app.get("/", response_class=HTMLResponse)
async def read_root():
    return """<h1>Welcome to GeoJSON Streaming API</h1>
              <p>Use /api/geojson endpoint with bounding box parameters to get combined features.</p>"""

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
