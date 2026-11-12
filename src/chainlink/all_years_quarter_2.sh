#!/bin/zsh

# Backup original config
cp configs/config_parcel_historic_2.yaml configs/config_parcel_historic_2.yaml.bak

for year in {2005..2010}; do
    echo "Processing year $year..."

    # Replace YEAR=XXXX with current year in config.txt using sed
    sed "s/linked_parcel_historic_[0-9]\{4\}/linked_parcel_historic_$year/" configs/config_parcel_historic_2.yaml.bak |
    sed "s/dfc_fulladd_[0-9]\{4\}/dfc_fulladd_$year/" > configs/config_parcel_historic_2.yaml
    # Run your command (replace with your actual command)
    # python process_data.py
    chainlink configs/config_parcel_historic_2.yaml

    echo "Finished processing year $year"
done

# Restore original config

echo "All years processed."
