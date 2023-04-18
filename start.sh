export VM_IMAGE_DIR="/opt/downloads/VantageExpress17.20_Sles12"
DEFAULT_VM_NAME="vantage-express"
VM_NAME="${VM_NAME:-$DEFAULT_VM_NAME}"
vboxmanage startvm "$VM_NAME" --type headless
export project_id="searce-practice-data-analytics"
echo "==> project-Id",$project_id
export bucket_name="snowflakes_pradu"
echo "==>bucket_name",$bucket_name
export dataset_name="nishi"
echo "==>dataset_name",$dataset_name
export host="localhost"
export username="nishi"
export password="nishi123"
export database="nishi"
export sink_bucket_teradata="snowflakes_pradu"
export audit="timestampcol"

echo "Please select a script to execute:"
echo "1. Historic-load"
echo "2. Incremental-load"
echo "3. Schema evolution"
echo "4. Create table in bigquery"
read -p "Enter the script number: " script_number

# Execute the selected script

case "$script_number" in
  "1")
    echo "Executing Historic Migration script..........."
    python3 batch_fastexport.py
    ;;
  "2")
    echo "Executing Incremental Migration script........"
    python3 Incremental_fastexport.py
    ;;
  "3")
    echo "Executing Schema Evolution Migration script......"
    python3 main.py --source schema_evolution
    ;;
  "4")
     echo "Executing Schema Creation script in Bigquery"
     python3 main.py --source teradata
     ;;
esac
