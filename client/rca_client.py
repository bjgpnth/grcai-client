# client/rca_client.py
"""
RCA Client - HTTP client wrapper for Central Reasoning Service.
Implements same interface as LLMReasoner for drop-in replacement with feature flag support.
"""

import os
import json
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class RCAClient:
    """
    HTTP client for Central Reasoning Service.
    Implements same interface as LLMReasoner for compatibility.
    """
    
    def __init__(
        self,
        central_url: str,
        api_key: str,
        model: str = "gpt-4o-mini",
        environment: str = None,
        timeout: int = 300  # 5 minutes for LLM calls
    ):
        """
        Initialize RCA Client.
        
        Args:
            central_url: Central service URL (e.g., "http://localhost:8000")
            api_key: OpenAI API key (passed to Central service)
            model: Model name (not used by client, but kept for interface compatibility)
            environment: Environment name (e.g., "qa", "prod")
            timeout: Request timeout in seconds (default: 300 = 5 minutes)
        """
        # Ensure URL doesn't end with /
        self.central_url = central_url.rstrip('/')
        self.api_key = api_key
        self.model = model  # Kept for interface compatibility, but Central uses it
        self.environment = environment
        self.timeout = timeout
        self._last_analyze_tasks = []  # Store tasks from last analyze() call
        
        # Create session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,  # 3 retries
            backoff_factor=1,  # Will be overridden by our custom backoff
            status_forcelist=[500, 502, 503, 504],  # Retry on these status codes
            allowed_methods=["POST"]  # Only retry POST requests
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def _make_request(
        self,
        endpoint: str,
        evidence: Dict[str, Any],
        retry_delays: List[int] = [5, 10, 30]
    ) -> Dict[str, Any]:
        """
        Make HTTP request to Central service with retry logic.
        
        Args:
            endpoint: API endpoint (e.g., "/api/v1/rca/analyze")
            evidence: Evidence dictionary
            retry_delays: List of retry delays in seconds (default: [5, 10, 30])
        
        Returns:
            Response dictionary
        
        Raises:
            Exception: If all retries fail
        """
        url = f"{self.central_url}{endpoint}"
        
        # Prepare request
        payload = {
            "evidence": evidence,
            "environment": self.environment
        }
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Retry logic with custom exponential backoff
        last_exception = None
        for attempt in range(len(retry_delays) + 1):  # +1 for initial attempt
            try:
                logger.info(f"Calling Central service: {url} (attempt {attempt + 1})")
                start_time = time.time()
                
                response = self.session.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout
                )
                
                elapsed = time.time() - start_time
                logger.info(f"Central service response: {response.status_code} ({elapsed:.2f}s)")
                
                # Handle successful response
                if response.status_code == 200:
                    return response.json()
                
                # Handle client errors (4xx) - don't retry
                if 400 <= response.status_code < 500:
                    error_msg = response.text
                    try:
                        error_json = response.json()
                        error_msg = error_json.get("detail", error_msg)
                    except:
                        pass
                    raise Exception(f"Client error {response.status_code}: {error_msg}")
                
                # Handle server errors (5xx) - will retry if attempts remaining
                if response.status_code >= 500:
                    error_msg = f"Server error {response.status_code}: {response.text}"
                    logger.warning(error_msg)
                    if attempt < len(retry_delays):
                        delay = retry_delays[attempt]
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                        continue
                    raise Exception(error_msg)
                
                # Unexpected status code
                raise Exception(f"Unexpected status code {response.status_code}: {response.text}")
                
            except requests.exceptions.Timeout:
                error_msg = f"Request timeout after {self.timeout}s"
                logger.warning(error_msg)
                if attempt < len(retry_delays):
                    delay = retry_delays[attempt]
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    last_exception = Exception(error_msg)
                    continue
                raise Exception(error_msg)
            
            except requests.exceptions.ConnectionError as e:
                # Don't expose the URL in error message - use user-friendly message
                error_msg = "Unable to connect to reasoning service. Please check if the service is available."
                logger.warning(f"Connection error: {str(e)}")  # Log full details for debugging
                if attempt < len(retry_delays):
                    delay = retry_delays[attempt]
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    last_exception = Exception(error_msg)
                    continue
                raise Exception(error_msg)
            
            except requests.exceptions.RetryError as e:
                # Handle retry exhaustion (e.g., too many 502 errors)
                error_msg = "Reasoning service is currently unavailable. Please try again later."
                logger.warning(f"Retry error after all attempts: {str(e)}")  # Log full details for debugging
                raise Exception(error_msg)
            
            except Exception as e:
                # For other exceptions, don't retry
                logger.error(f"Request failed: {e}", exc_info=True)
                raise
        
        # If we exhausted all retries
        if last_exception:
            raise last_exception
        raise Exception("Request failed after all retries")
    
    def analyze(self, evidence_file: str, progress_callback=None) -> str:
        """
        Analyze evidence file and generate RCA report.
        
        Args:
            evidence_file: Path to evidence JSON file
            progress_callback: Optional progress callback (not used in HTTP client, kept for compatibility)
        
        Returns:
            RCA report text
        """
        # Load evidence from file
        with open(evidence_file, 'r', encoding='utf-8') as f:
            evidence = json.load(f)
        
        # Call Central service
        result = self._make_request("/api/v1/rca/analyze", evidence)
        
        if result.get("status") != "success":
            error = result.get("error", "Unknown error")
            return f"⚠️ Error from Central service: {error}"
        
        # Store tasks for generate_tasks_from_rca() if called later
        self._last_analyze_tasks = result.get("tasks", [])
        
        return result.get("rca_text", "")
    
    def generate_incident_response_report(self, evidence_file: str, progress_callback=None) -> str:
        """
        Generate Incident Response report from evidence file.
        
        Args:
            evidence_file: Path to evidence JSON file
            progress_callback: Optional progress callback (not used in HTTP client, kept for compatibility)
        
        Returns:
            Incident Response report text
        """
        # Load evidence from file
        with open(evidence_file, 'r', encoding='utf-8') as f:
            evidence = json.load(f)
        
        # Call Central service
        result = self._make_request("/api/v1/rca/incident-response", evidence)
        
        if result.get("status") != "success":
            error = result.get("error", "Unknown error")
            return f"⚠️ Error from Central service: {error}"
        
        return result.get("ir_text", "")
    
    def generate_tasks_from_rca(self, rca_text: str) -> List[Dict[str, Any]]:
        """
        Generate tasks from RCA text.
        
        Note: In the current Central service API, tasks are returned directly
        from analyze() endpoint. This method returns tasks from the last
        analyze() call if available.
        
        Args:
            rca_text: RCA text (kept for interface compatibility, but tasks come from analyze endpoint)
        
        Returns:
            List of task dictionaries from last analyze() call
        """
        # Return tasks from last analyze() call
        # This matches the pattern in orchestrator where analyze() is called first,
        # then generate_tasks_from_rca() is called with the result
        return getattr(self, '_last_analyze_tasks', [])


def create_rca_client(api_key: str, model: str = "gpt-4o-mini", environment: str = None) -> RCAClient:
    """
    Factory function to create RCAClient with configuration from environment.
    
    Args:
        api_key: OpenAI API key
        model: Model name
        environment: Environment name
    
    Returns:
        RCAClient instance
    """
    central_url = os.getenv("GRCAI_CENTRAL_URL", "http://localhost:8000")
    
    return RCAClient(
        central_url=central_url,
        api_key=api_key,
        model=model,
        environment=environment
    )


def get_reasoning_client(api_key: str, model: str = "gpt-4o-mini", environment: str = None):
    """
    Get reasoning client (RCAClient or LLMReasoner) based on feature flag.
    
    Args:
        api_key: OpenAI API key
        model: Model name
        environment: Environment name
    
    Returns:
        RCAClient if remote mode, LLMReasoner if local mode
    """
    reasoning_mode = os.getenv("GRCAI_REASONING_MODE", "remote").lower()
    
    if reasoning_mode == "local":
        # Import here to avoid dependency when using remote mode
        from llm.llm_reasoner import LLMReasoner
        logger.info("Using local LLMReasoner (development mode)")
        return LLMReasoner(api_key=api_key, model=model, environment=environment)
    else:
        # Default to remote mode
        logger.info("Using remote RCAClient (production mode)")
        return create_rca_client(api_key=api_key, model=model, environment=environment)

