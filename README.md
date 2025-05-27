# FastLanes\_Data

FastLanes\_Data is a collection of benchmark datasets and utility scripts designed to streamline performance testing and
analysis of the [FastLanes](https://github.com/cwida/FastLanes) file format. It brings together real-world and synthetic
datasets to test FastLanes in all cases.

## Datasets

* **NextiaJD**:
* **public\_bi**: Sample of [public BI dataset](https://github.com/cwida/public_bi_benchmark).
* **issues/cwida/alp/37**: Data extracted for issue #37 in the [ALP](https://github.com/cwida/ALP) project.

## Requirements

* **Python**: Version 3.8 or higher
* **Bash**: For shell-based export scripts
* Optional: Any additional dependencies listed in `requirements.txt` (if present)

**Export the data directory** using the provided script:

```bash
source scripts/export_fastlanes_data_dir.sh /path/to/your/data
```

This sets the `$FASTLANES_DATA_DIR` environment variable to the root path of your datasets.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

Come talk to us if you like FastLanes or Data!

[![Discord](https://img.shields.io/discord/discord.gg/gwx87YYn?label=Discord\&style=flat-square)](https://discord.gg/gwx87YYn)
