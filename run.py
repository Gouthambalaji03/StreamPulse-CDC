import uvicorn
import os
import sys

if __name__ == "__main__":
    project_root = os.path.dirname(os.path.abspath(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
        
    print("=" * 70)
    print("  [*] Starting CDC Stream: Database Change Streaming Platform")
    print("  [*] Dashboard: http://localhost:8000")
    print("  [*] WebSocket: ws://localhost:8000/ws/events")
    print("  [*] API Docs:  http://localhost:8000/docs")
    print("=" * 70)
    
    uvicorn.run("backend.main:app", host="127.0.0.1", port=8000, reload=False)
