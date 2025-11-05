from concurrent.futures import ThreadPoolExecutor, as_completed
from app.helpers.downloader import download_file
from app.processors.models_data import models_list


def download_model_task(model_data):
    """Download a single model file."""
    return download_file(
        model_data["model_name"],
        model_data["local_path"],
        model_data["hash"],
        model_data["url"],
    )


def download_all_models_parallel(models_list, max_workers=4):
    """
    Download all models in parallel using ThreadPoolExecutor.

    Args:
        models_list: List of model data dictionaries
        max_workers: Maximum number of concurrent downloads (default: 4)
    """
    print(f"\nStarting parallel download of {len(models_list)} models with {max_workers} workers...\n")

    successful = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_model = {
            executor.submit(download_model_task, model_data): model_data["model_name"]
            for model_data in models_list
        }

        # Collect results as they complete
        for future in as_completed(future_to_model):
            model_name = future_to_model[future]
            try:
                result = future.result()
                if result:
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"\nException downloading {model_name}: {e}")
                failed += 1

    print(f"\n{'='*60}")
    print(f"Download Summary:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(models_list)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    download_all_models_parallel(models_list, max_workers=4)
