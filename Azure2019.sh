#!/usr/bin/env bash
#
# Script to download and process Azure Functions dataset 2019.
# Follows Google Shell Style Guide: https://google.github.io/styleguide/shellguide.html
# Bash 3 compatible implementation

set -euo pipefail

readonly URL="https://azurepublicdatasettraces.blob.core.windows.net/azurepublicdatasetv2/azurefunctions_dataset2019/azurefunctions-dataset2019.tar.xz"
readonly FILENAME="azurefunctions-dataset2019.tar.xz"
readonly DATA_DIR="./azure2019"
readonly HASH_MAP_FILE="${DATA_DIR}/hash_map.txt"
readonly INVOCATIONS_FILE="${DATA_DIR}/invocations.csv"
readonly TRANSLATION_FILE="${DATA_DIR}/translation.csv"
readonly MAX_WORKERS=7  # Similar to the Python version's parallelism

# Ensure backward compatibility with Bash 3
if [ "${BASH_VERSION:0:1}" -lt 4 ]; then
  # Bash 3 doesn't support associative arrays, so we'll use other techniques
  readonly BASH3_MODE=true
else
  readonly BASH3_MODE=false
fi

function error() {
  echo "[ERROR] $*" >&2
}

function info() {
  echo "[INFO] $*"
}

function debug() {
  if [ "${VERBOSE:-0}" -eq 1 ]; then
    echo "[DEBUG] $*" >&2
  fi
}

function download_and_extract_dataset() {
  info "Downloading Azure Functions dataset 2019..."
  if ! curl -L "${URL}" -o "${FILENAME}"; then
    error "Failed to download dataset"
    return 1
  fi

  # Create directory
  mkdir -p "${DATA_DIR}"

  info "Extracting files..."
  if ! tar -xf "${FILENAME}" -C "${DATA_DIR}/"; then
    error "Failed to extract files"
    return 1
  fi

  # Remove the archive file
  info "Cleaning up..."
  rm "${FILENAME}"

  info "Dataset extracted successfully to ${DATA_DIR}/"
  return 0
}

function create_hash_id() {
  local owner="$1"
  local app="$2"
  local func="$3"
  local combined="${owner}_${app}_${func}"

  # Use md5sum to create a hash and take first 10 characters
  echo "${combined}" | md5sum | cut -c 1-10
}

function find_invocation_files() {
  # Find all invocation files
  local files
  files=$(find "${DATA_DIR}" -name "invocations_per_function_md.anon.d*.csv" | sort)
  echo "${files}"
}

function initialize_output_files() {
  # Create output files with headers
  echo "uuid,call_count" > "${INVOCATIONS_FILE}"
  echo "HashOwner,HashApp,HashFunction,Trigger,original_uuid,new_hash_uuid" > "${TRANSLATION_FILE}"

  # Create temp directory for intermediate files
  mkdir -p "${DATA_DIR}/temp"
}

function process_invocation_file() {
  local file="$1"
  local file_idx="$2"
  local base_timestamp=$((file_idx * 24 * 60))
  local filename
  filename=$(basename "${file}")
  local temp_invocations="${DATA_DIR}/temp/${filename}_invocations.txt"
  local temp_translations="${DATA_DIR}/temp/${filename}_translations.txt"

  info "Processing file: ${filename} (${file_idx})"

  # Skip header line
  tail -n +2 "${file}" | while IFS=, read -r owner app func trigger min1 min2 min3 remainder; do
    # Create a unique identifier for this function
    local func_id
    func_id=$(create_hash_id "${owner}" "${app}" "${func}")

    # Add to translation data
    echo "${owner},${app},${func},${trigger},${owner}_${app}_${func},${func_id}" >> "${temp_translations}"

    # Process invocation counts - this is a simplified approach
    # In a real implementation, you'd need to parse all 1440 minute columns
    # Here we just demonstrate the concept with the first few columns
    local timestamps=""

    # We need to properly split the remainder into individual minute values
    # This is a simplified version that processes just a few columns
    local full_line="${min1},${min2},${min3},${remainder}"

    # Use AWK to process all columns - this scales better than Bash loops for 1440 columns
    awk -v base="${base_timestamp}" -v func_id="${func_id}" '
      BEGIN { FS="," }
      {
        for (i=4; i<=NF+3; i++) {
          count = $i
          if (count > 0) {
            minute = i - 3
            timestamp = base + minute
            for (j=0; j<count; j++) {
              print func_id "," timestamp
            }
          }
        }
      }
    ' <<< "${full_line}" >> "${temp_invocations}"
  done

  info "Completed processing ${filename}"
}

function merge_results() {
  info "Merging invocation results..."

  # Process all temp invocation files to count calls per function
  info "Counting function calls..."
  cat "${DATA_DIR}/temp/"*"_invocations.txt" | sort | uniq -c | awk '{print $2","$1}' > "${INVOCATIONS_FILE}.tmp"

  # Add header and replace main file
  cat "${INVOCATIONS_FILE}" "${INVOCATIONS_FILE}.tmp" > "${INVOCATIONS_FILE}.new"
  mv "${INVOCATIONS_FILE}.new" "${INVOCATIONS_FILE}"
  rm "${INVOCATIONS_FILE}.tmp"

  info "Merging translation results..."
  # Merge all translation files and remove duplicates
  sort "${DATA_DIR}/temp/"*"_translations.txt" | uniq > "${TRANSLATION_FILE}.tmp"

  # Add header and replace main file
  cat "${TRANSLATION_FILE}" "${TRANSLATION_FILE}.tmp" > "${TRANSLATION_FILE}.new"
  mv "${TRANSLATION_FILE}.new" "${TRANSLATION_FILE}"
  rm "${TRANSLATION_FILE}.tmp"

  info "Results merged successfully"
}

function process_dataset_parallel() {
  info "Finding invocation files..."
  local invocation_files
  invocation_files=$(find_invocation_files)
  local file_count
  file_count=$(echo "${invocation_files}" | wc -l)

  info "Found ${file_count} invocation files"

  # Initialize output files
  initialize_output_files

  # Process files with parallel execution if GNU Parallel is available
  if command -v parallel >/dev/null 2>&1; then
    info "Using GNU Parallel to process files with ${MAX_WORKERS} workers..."

    # Export functions so parallel can use them
    export -f process_invocation_file create_hash_id info error debug
    export DATA_DIR

    # Create a temp file with file paths and indices
    local temp_file_list="${DATA_DIR}/temp/file_list.txt"
    local idx=0
    echo "${invocation_files}" | while read -r file; do
      echo "${file} ${idx}"
      idx=$((idx + 1))
    done > "${temp_file_list}"

    # Run in parallel
    parallel --verbose -j "${MAX_WORKERS}" --colsep ' ' 'process_invocation_file {1} {2}' :::: "${temp_file_list}"

    # Cleanup
    rm "${temp_file_list}"
  else
    # Sequential processing if parallel is not available
    info "GNU Parallel not found, processing files sequentially (this will be slow)..."
    local idx=0
    echo "${invocation_files}" | while read -r file; do
      process_invocation_file "${file}" "${idx}"
      idx=$((idx + 1))
    done
  fi

  # Merge all the temp files
  merge_results

  # Cleanup temp directory
  rm -rf "${DATA_DIR}/temp"

  info "Dataset processing complete"
  info "Results saved to:"
  info "  - Invocations: ${INVOCATIONS_FILE}"
  info "  - Translations: ${TRANSLATION_FILE}"
}

function check_dependencies() {
  local missing_deps=()

  # Check for curl
  if ! command -v curl >/dev/null 2>&1; then
    missing_deps+=("curl")
  fi

  # Check for tar
  if ! command -v tar >/dev/null 2>&1; then
    missing_deps+=("tar")
  fi

  # Check for md5sum
  if ! command -v md5sum >/dev/null 2>&1; then
    missing_deps+=("md5sum")
  fi

  # Check for awk
  if ! command -v awk >/dev/null 2>&1; then
    missing_deps+=("awk")
  fi

  # Check for find
  if ! command -v find >/dev/null 2>&1; then
    missing_deps+=("find")
  fi

  # Return error if any dependencies are missing
  if [ ${#missing_deps[@]} -gt 0 ]; then
    error "Missing dependencies: ${missing_deps[*]}"
    error "Please install the required dependencies and try again."
    return 1
  fi

  # Optional dependency check
  if ! command -v parallel >/dev/null 2>&1; then
    info "GNU Parallel not found. Processing will be sequential and slower."
    info "Install GNU Parallel for faster processing: sudo apt-get install parallel"
  fi

  info "All required dependencies are installed."
  return 0
}

function create_summary() {
  local total_functions
  total_functions=$(tail -n +2 "${INVOCATIONS_FILE}" | wc -l)

  local total_invocations
  total_invocations=$(tail -n +2 "${INVOCATIONS_FILE}" | awk -F, '{sum+=$2} END {print sum}')

  local top_functions
  top_functions=$(tail -n +2 "${INVOCATIONS_FILE}" | sort -t, -k2,2nr | head -10)

  local summary_file="${DATA_DIR}/summary.txt"

  {
    echo "=== Azure Functions 2019 Dataset Summary ==="
    echo "Total functions: ${total_functions}"
    echo "Total invocations: ${total_invocations}"
    echo ""
    echo "Top 10 functions by invocation count:"
    echo "UUID,Count"
    echo "${top_functions}"
  } > "${summary_file}"

  info "Summary created at: ${summary_file}"
  cat "${summary_file}"
}

function main() {
  # Banner
  echo "====================================================="
  echo "  Azure Functions Dataset Processor (Bash 3 Version)"
  echo "====================================================="

  # Check dependencies
  if ! check_dependencies; then
    exit 1
  fi

  # Download and extract dataset if not exists
  if [ ! -d "${DATA_DIR}" ] || [ -z "$(ls -A "${DATA_DIR}" 2>/dev/null)" ]; then
    if ! download_and_extract_dataset; then
      exit 1
    fi
  else
    info "Dataset directory already exists: ${DATA_DIR}"
  fi

  # Process dataset
  if ! process_dataset_parallel; then
    error "Failed to process dataset"
    exit 1
  fi

  # Create summary
  create_summary

  info "Processing completed successfully!"
  return 0
}

# Execute main function
main