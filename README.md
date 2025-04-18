# Azure Function: Aggregate Logs

This Azure Function is the **third component** of the *Vehicle Speed Calculation* project. It aggregates results from all 18 `.log` files generated by the YOLOv8-based analysis of video segments.

## Trigger
The function is triggered when a `.log` file is uploaded to the `test-logs/` container in Azure Blob Storage.

## What It Does
- Waits until all `segment_XXX.mp4.log` files (18 in total) are available.
- Parses each log for:
  - Total vehicles per direction
  - Speed violations
  - Average speeds
  - Per-vehicle details (ID, type, direction, speed, time)
- Uploads vehicle data to **Azure Cosmos DB**.
- Computes and logs:
  - Global totals
  - 5-minute interval summaries
- Generates a `total.log` file containing all summaries and vehicle details.
- Uploads `total.log` to Blob Storage.

## Tech Stack
- Azure Functions (Python)
- Azure Blob Storage
- Azure Cosmos DB
- Regex for log parsing