#!/bin/bash

# monitoring/deploy_alerts.sh
# Deploy Cloud Monitoring Alert Policies - No Manual Client Management
# ✅ Uses gcloud CLI instead of manual MetricServiceClient
# ✅ Declarative alert policy deployment

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-asia-southeast1}"

if [[ -z "$PROJECT_ID" ]]; then
    echo "❌ Error: PROJECT_ID environment variable is required"
    echo "Usage: export PROJECT_ID=your-project && ./deploy_alerts.sh"
    exit 1
fi

echo "🚀 Deploying Cloud Monitoring Alert Policies..."
echo "📍 Project: $PROJECT_ID"
echo "🌏 Region: $REGION"

# Create alert policies from YAML
ALERT_POLICIES=(
    "dataflow-window-latency"
    "bigquery-storage-write-performance"
    "pubsub-backlog"
    "pipeline-error-rate"
    "secret-manager-access-failures"
)

# Extract JSON policies from the multi-document YAML
awk '
BEGIN { RS="---"; doc=0 }
/^# / { next }
/^\s*$/ { next }
/{/ { 
    doc++
    filename = "alert_policy_" doc ".json"
    print $0 > filename
    close(filename)
}
' alert_policies.yaml

echo "📝 Extracted individual alert policy files..."

# Deploy each alert policy
for i in {1..5}; do
    if [[ -f "alert_policy_${i}.json" ]]; then
        echo "⚡ Deploying alert policy ${i}..."
        
        # Deploy the alert policy
        gcloud alpha monitoring policies create \
            --policy-from-file="alert_policy_${i}.json" \
            --project="$PROJECT_ID" \
            --quiet || {
                echo "⚠️  Warning: Alert policy ${i} may already exist, updating..."
                # Try to update existing policy (requires policy ID)
                POLICY_ID=$(gcloud alpha monitoring policies list \
                    --filter="displayName:*Current*" \
                    --format="value(name)" \
                    --project="$PROJECT_ID" | head -1)
                
                if [[ -n "$POLICY_ID" ]]; then
                    gcloud alpha monitoring policies update "$POLICY_ID" \
                        --policy-from-file="alert_policy_${i}.json" \
                        --project="$PROJECT_ID" \
                        --quiet || echo "❌ Failed to update policy ${i}"
                fi
            }
        
        # Clean up temporary file
        rm -f "alert_policy_${i}.json"
    fi
done

echo "✅ Alert policies deployment completed!"

# Create notification channels if they don't exist
echo "📢 Setting up notification channels..."

# Slack notification channel
SLACK_CHANNEL_ID=$(gcloud alpha monitoring channels list \
    --filter="displayName:Data Platform Slack" \
    --format="value(name)" \
    --project="$PROJECT_ID" | head -1)

if [[ -z "$SLACK_CHANNEL_ID" ]]; then
    echo "🔧 Creating Slack notification channel..."
    gcloud alpha monitoring channels create \
        --display-name="Data Platform Slack" \
        --type="slack" \
        --channel-labels="channel_name=#data-platform-alerts" \
        --project="$PROJECT_ID" || echo "⚠️  Manual Slack channel setup required"
fi

# Email notification channel  
EMAIL_CHANNEL_ID=$(gcloud alpha monitoring channels list \
    --filter="displayName:On-Call Email" \
    --format="value(name)" \
    --project="$PROJECT_ID" | head -1)

if [[ -z "$EMAIL_CHANNEL_ID" ]]; then
    echo "📧 Creating email notification channel..."
    gcloud alpha monitoring channels create \
        --display-name="On-Call Email" \
        --type="email" \
        --channel-labels="email_address=oncall@company.com" \
        --project="$PROJECT_ID" || echo "⚠️  Manual email channel setup required"
fi

echo "📊 Monitoring setup summary:"
echo "✅ Alert Policies: $(gcloud alpha monitoring policies list --project="$PROJECT_ID" --filter="displayName:*Current*" --format="value(displayName)" | wc -l) deployed"
echo "✅ Notification Channels: $(gcloud alpha monitoring channels list --project="$PROJECT_ID" --format="value(displayName)" | wc -l) configured"

echo ""
echo "🎉 Cloud Monitoring deployment completed!"
echo "📈 View alerts: https://console.cloud.google.com/monitoring/alerting?project=$PROJECT_ID"
echo "🔔 Configure notification channels: https://console.cloud.google.com/monitoring/settings/notifications?project=$PROJECT_ID"
