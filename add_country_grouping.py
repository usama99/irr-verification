#!/usr/bin/env python3
"""
Add Country Grouping to Enriched Links
Groups peers by country for efficient verification
"""

import json
from collections import defaultdict

# ============================================
# CONFIGURATION
# ============================================

INPUT_FILE = '../2025OctUpdatedCode/SIGCOMM/enriched_links.json'
OUTPUT_FILE = '../2025OctUpdatedCode/SIGCOMM/enriched_links_grouped.json'


def group_peers_by_country(enriched_data):
    """
    Group peers by country for each AS

    Input structure:
    {
      "1234": {
        "asn": "1234",
        "as_info": {...},
        "links": [
          {"peer_as": "5678", "peer_country": "United States", ...},
          {"peer_as": "9999", "peer_country": "Singapore", ...}
        ]
      }
    }

    Output adds:
    {
      "1234": {
        "asn": "1234",
        "as_info": {...},
        "links": [...],
        "grouped_by_country": {
          "United States": {
            "count": 52,
            "peers": [list of links]
          },
          "Singapore": {
            "count": 78,
            "peers": [list of links]
          }
        }
      }
    }
    """
    print("Grouping peers by country for each AS...")

    for asn, data in enriched_data.items():
        # Group links by peer country
        country_groups = defaultdict(list)

        for link in data.get('links', []):
            peer_country = link.get('peer_country', 'Unknown')
            country_groups[peer_country].append(link)

        # Sort countries by peer count (descending)
        sorted_countries = sorted(
            country_groups.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )

        # Create grouped structure
        data['grouped_by_country'] = {}
        for country, peers in sorted_countries:
            data['grouped_by_country'][country] = {
                'count': len(peers),
                'peers': peers
            }

    print(f"✓ Grouped peers for {len(enriched_data)} ASes")
    return enriched_data


def generate_statistics(enriched_data):
    """Generate statistics about country grouping"""

    total_ases = len(enriched_data)
    total_countries = set()
    country_peer_counts = defaultdict(int)

    for data in enriched_data.values():
        for country, group in data.get('grouped_by_country', {}).items():
            total_countries.add(country)
            country_peer_counts[country] += group['count']

    top_countries = sorted(
        country_peer_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )[:15]

    print("\n" + "=" * 60)
    print("COUNTRY GROUPING STATISTICS")
    print("=" * 60)
    print(f"Total ASes: {total_ases:,}")
    print(f"Total unique peer countries: {len(total_countries)}")
    print(f"\nTop 15 Peer Countries by Link Count:")
    for i, (country, count) in enumerate(top_countries, 1):
        print(f"  {i}. {country}: {count:,} peer links")
    print("=" * 60)


def main():
    print("=" * 60)
    print("Add Country Grouping to Enriched Links")
    print("=" * 60)
    print(f"Input: {INPUT_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print()

    # Load enriched data
    print("Loading enriched links...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        enriched_data = json.load(f)
    print(f"✓ Loaded {len(enriched_data)} ASes")

    # Add country grouping
    enriched_data = group_peers_by_country(enriched_data)

    # Generate statistics
    generate_statistics(enriched_data)

    # Save grouped data
    print(f"\nSaving grouped data to: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(enriched_data, f, indent=2, ensure_ascii=False)
    print("✓ Saved grouped data")

    print("\n" + "=" * 60)
    print("✓ GROUPING COMPLETE!")
    print("=" * 60)
    print(f"\nNext step:")
    print(f"Use {OUTPUT_FILE} in the portal")
    print("Deploy and operators will see country-grouped peers!")


if __name__ == '__main__':
    main()