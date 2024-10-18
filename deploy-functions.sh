# schema setup
echo "======================================================"
echo "deploying the schema setup"
echo "======================================================"

gcloud functions deploy dev-schema-setup_stock \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/jasminek/ba882_group_project/functions/schema_setup \
    --stage-bucket jasminek_ba882 \
    --service-account ba882-664@ba882-435919.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 256MB 