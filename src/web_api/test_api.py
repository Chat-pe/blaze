#!/usr/bin/env python3
"""
Simple test script for the Blaze Web API
"""

import requests
import json
import sys
from datetime import datetime, timedelta

API_BASE_URL = "http://localhost:8000"

def test_endpoint(endpoint, method="GET", data=None, expected_status=200):
    """Test a single API endpoint"""
    url = f"{API_BASE_URL}{endpoint}"
    
    try:
        if method == "GET":
            response = requests.get(url)
        elif method == "POST":
            response = requests.post(url, json=data)
        else:
            print(f"❌ Unsupported method: {method}")
            return False
        
        if response.status_code == expected_status:
            print(f"✅ {method} {endpoint} - Status: {response.status_code}")
            return True
        else:
            print(f"❌ {method} {endpoint} - Expected: {expected_status}, Got: {response.status_code}")
            print(f"   Response: {response.text[:200]}...")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"❌ {method} {endpoint} - Connection failed. Is the API server running?")  
        return False
    except Exception as e:
        print(f"❌ {method} {endpoint} - Error: {str(e)}")
        return False

def test_api():
    """Run all API tests"""
    print("🚀 Testing Blaze Web API")
    print("=" * 50)
    
    tests = [
        # Basic endpoints
        ("/", "GET", None, 200),
        ("/health", "GET", None, [200, 503]),  # Can be either healthy or unhealthy
        ("/debug/system-info", "GET", None, 200),
        ("/debug/files", "GET", None, 200),
        
        # System endpoints
        ("/status", "GET", None, [200, 503]),
        ("/blocks", "GET", None, [200, 503]),
        
        # Sequence endpoints
        ("/sequences", "GET", None, 200),
        ("/jobs", "GET", None, 200),
    ]
    
    success_count = 0
    total_count = len(tests)
    
    for endpoint, method, data, expected_status in tests:
        # Handle multiple expected status codes
        if isinstance(expected_status, list):
            success = False
            for status in expected_status:
                if test_endpoint(endpoint, method, data, status):
                    success = True
                    break
            if success:
                success_count += 1
        else:
            if test_endpoint(endpoint, method, data, expected_status):
                success_count += 1
    
    print("\n" + "=" * 50)
    print(f"🎯 Test Results: {success_count}/{total_count} passed")
    
    if success_count == total_count:
        print("🎉 All tests passed!")
        return True
    else:
        print("⚠️  Some tests failed")
        return False

def test_specific_sequence():
    """Test sequence-specific endpoints if sequences exist"""
    print("\n🔍 Testing sequence-specific endpoints...")
    
    try:
        # Get available sequences
        response = requests.get(f"{API_BASE_URL}/sequences")
        if response.status_code == 200:
            data = response.json()
            sequences = data.get("sequences", [])
            
            if sequences:
                seq_id = sequences[0]["seq_id"]
                print(f"📋 Testing with sequence: {seq_id}")
                
                # Test sequence status
                test_endpoint(f"/sequences/{seq_id}/status", "GET", None, 200)
                
                # Test sequence logs
                test_endpoint(f"/sequences/{seq_id}/logs", "GET", None, 200)
            else:
                print("📭 No sequences found to test")
        else:
            print("❌ Could not retrieve sequences list")
            
    except Exception as e:
        print(f"❌ Error testing sequences: {str(e)}")

def test_job_submission():
    """Test job submission if system is running"""
    print("\n📤 Testing job submission...")
    
    try:
        # Check if system is running
        response = requests.get(f"{API_BASE_URL}/status")
        if response.status_code == 200:
            data = response.json()
            sequences = data.get("sequences", [])
            
            if sequences:
                seq_id = sequences[0]
                print(f"🎯 Testing job submission for sequence: {seq_id}")
                
                job_data = {
                    "seq_id": seq_id,
                    "parameters": {"test": "value"},
                    "seq_run_interval": "*/10 * * * *",
                    "start_date": datetime.now().isoformat(),
                    "end_date": (datetime.now() + timedelta(hours=1)).isoformat()
                }
                
                test_endpoint("/jobs/submit", "POST", job_data, 200)
            else:
                print("📭 No sequences available for job submission test")
        else:
            print("🔴 System not running - skipping job submission test")
            
    except Exception as e:
        print(f"❌ Error testing job submission: {str(e)}")

def show_api_info():
    """Show detailed API information"""
    print("\n📊 API Information:")
    print("-" * 30)
    
    try:
        response = requests.get(f"{API_BASE_URL}/")
        if response.status_code == 200:
            data = response.json()
            print(f"API: {data.get('message', 'Unknown')}")
            print(f"Version: {data.get('version', 'Unknown')}")
            print("\nAvailable endpoints:")
            endpoints = data.get('endpoints', {})
            for name, path in endpoints.items():
                print(f"  • {name}: {path}")
        
        # Show debug info
        response = requests.get(f"{API_BASE_URL}/debug/system-info")
        if response.status_code == 200:
            data = response.json()
            print(f"\nSystem files status:")
            for key, value in data.items():
                print(f"  • {key}: {value}")
                
    except Exception as e:
        print(f"❌ Error getting API info: {str(e)}")

if __name__ == "__main__":
    print("🧪 Blaze Web API Test Suite")
    print("=" * 60)
    
    # Show API information
    show_api_info()
    
    # Run basic tests
    success = test_api()
    
    # Run sequence-specific tests
    test_specific_sequence()
    
    # Test job submission (only if system is healthy)
    test_job_submission()
    
    print("\n" + "=" * 60)
    print("✨ Test suite completed!")
    
    if not success:
        sys.exit(1) 