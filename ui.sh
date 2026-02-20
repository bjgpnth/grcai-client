#!/bin/sh
# Start gRCAi Client Streamlit UI.
# Use from host or inside dev container. Set GRCAI_CENTRAL_URL and OPENAI_API_KEY (e.g. in .env or export).
echo "GRCAI_CENTRAL_URL: $GRCAI_CENTRAL_URL"
echo "OPENAI_API_KEY: $OPENAI_API_KEY"
exec streamlit run ui/app.py --server.address=0.0.0.0 --server.port=8501
