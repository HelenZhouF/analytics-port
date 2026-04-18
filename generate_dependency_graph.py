import json
import os
from collections import defaultdict

def normalize_name(name):
    """Normalize dataset name according to rules:
    - If contains '&', take substring before '&' (minus 1 position)
    - Case insensitive
    """
    if not name:
        return name
    name = name.strip()
    amp_pos = name.find('&')
    if amp_pos > 0:
        name = name[:amp_pos - 1] if amp_pos > 1 else name[:amp_pos]
    return name.upper()

def get_clean_filename(name):
    """Extract clean filename from include name (remove path prefixes)"""
    if not name:
        return name
    # Remove macro variables like &PGM_PATH. or &BACKEND_PATH.\MACRO\
    # Extract just the filename
    parts = name.replace('\\', '/').split('/')
    filename = parts[-1]
    # Handle cases like "&Macro_Path.Set_Macro.txt" -> "Set_Macro.txt"
    dot_pos = filename.find('.')
    if dot_pos > 0 and dot_pos < len(filename) - 1:
        # Check if it's a macro variable prefix like "Macro_Path."
        prefix = filename[:dot_pos]
        if prefix.startswith('&') or prefix.upper() in ['MACRO_PATH', 'PGM_PATH', 'BACKEND_PATH', 'FEE_REPORT_PATH', 'SCRIPT_PATH', 'DATA_PATH']:
            filename = filename[dot_pos + 1:]
    return filename

def load_json_files(output_dir):
    """Load all 4 JSON files: daily, monthly, quarterly, on_request"""
    json_files = ['daily.json', 'monthly.json', 'quarterly.json', 'on_request.json']
    all_data = {}
    
    for json_file in json_files:
        file_path = os.path.join(output_dir, json_file)
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_data.update(data)
            print(f"Loaded {json_file}: {len(data)} items")
    
    return all_data

def extract_include_dependencies(all_data):
    """Extract direct include dependencies.
    If node A's include list contains node B's filename, then A depends on B (A --> B)
    """
    include_deps = []
    
    # Create a mapping of filenames to their nodes (case-insensitive)
    filename_to_node = {}
    for node_name, node_data in all_data.items():
        # Store both .sas and potential .txt names
        clean_name = node_name.upper().replace('.SAS', '')
        filename_to_node[clean_name] = node_name
        filename_to_node[node_name.upper()] = node_name
    
    for node_name, node_data in all_data.items():
        includes = node_data.get('include', [])
        if not includes:
            continue
        
        for include_item in includes:
            include_name = include_item.get('name', '')
            if not include_name:
                continue
            
            # Clean the include name
            clean_include = get_clean_filename(include_name)
            
            # Check if this include references any node
            # Match against .sas nodes (include could be .txt or .sas)
            include_upper = clean_include.upper()
            
            # Try to match with different extensions
            potential_matches = [
                include_upper,
                include_upper.replace('.TXT', '.SAS'),
                include_upper.replace('.SAS', '.TXT'),
                include_upper.replace('.TXT', ''),
                include_upper.replace('.SAS', ''),
            ]
            
            matched_node = None
            for match in potential_matches:
                if match in filename_to_node:
                    matched_node = filename_to_node[match]
                    break
            
            if matched_node and matched_node != node_name:
                include_deps.append({
                    'source': node_name,
                    'target': matched_node,
                    'label': f'include: {clean_include}',
                    'type': 'include'
                })
    
    return include_deps

def extract_dependency_relationships(all_data):
    """Extract indirect dataset dependencies.
    If node A's output.ds name appears in node B's input.ds.name, then B depends on A.
    B -> .dataset_name. -> A
    """
    dependency_deps = []
    
    # First, collect all output datasets
    output_datasets = defaultdict(list)  # normalized_name -> list of (node_name, original_name, lib)
    
    for node_name, node_data in all_data.items():
        output = node_data.get('output', {})
        output_ds = output.get('ds', [])
        
        for ds in output_ds:
            ds_name = ds.get('name', '')
            if not ds_name:
                continue
            
            normalized = normalize_name(ds_name)
            if normalized:
                output_datasets[normalized].append({
                    'node': node_name,
                    'original_name': ds_name,
                    'lib': ds.get('lib', ''),
                    'libname': ds.get('libname', '')
                })
    
    # Now check input datasets against output datasets
    for node_name, node_data in all_data.items():
        input_data = node_data.get('input', {})
        input_ds = input_data.get('ds', [])
        
        for ds in input_ds:
            ds_name = ds.get('name', '')
            if not ds_name:
                continue
            
            normalized = normalize_name(ds_name)
            if not normalized:
                continue
            
            # Find all output nodes that produce this dataset
            if normalized in output_datasets:
                for output_info in output_datasets[normalized]:
                    source_node = output_info['node']
                    if source_node == node_name:
                        continue  # Skip self-dependencies
                    
                    dependency_deps.append({
                        'source': node_name,
                        'target': source_node,
                        'label': f'dataset: {output_info["original_name"]}',
                        'type': 'dependency',
                        'ds_info': {
                            'input_name': ds_name,
                            'input_lib': ds.get('lib', ''),
                            'output_name': output_info['original_name'],
                            'output_lib': output_info['lib'],
                            'output_libname': output_info['libname'],
                            'normalized_name': normalized
                        }
                    })
    
    return dependency_deps

def main():
    output_dir = r'c:\workspace\ai\relationship\analytics-port\output'
    
    print("Loading JSON files...")
    all_data = load_json_files(output_dir)
    print(f"Total nodes loaded: {len(all_data)}")
    
    print("\nExtracting include dependencies...")
    include_deps = extract_include_dependencies(all_data)
    print(f"Found {len(include_deps)} include dependencies")
    
    print("\nExtracting dataset dependencies...")
    dependency_deps = extract_dependency_relationships(all_data)
    print(f"Found {len(dependency_deps)} dataset dependencies")
    
    # Combine all dependencies
    all_dependencies = include_deps + dependency_deps
    
    # Prepare node information
    nodes_info = {}
    for node_name, node_data in all_data.items():
        nodes_info[node_name] = {
            'item_no': node_data.get('item_no', ''),
            'task': node_data.get('task', ''),
            'path': node_data.get('path', ''),
            'comments': node_data.get('comments', ''),
            'owner': node_data.get('owner', ''),
            'rows': node_data.get('rows', 0),
            'main_program': node_data.get('main_program', ''),
            'type': node_data.get('type', '')
        }
    
    # Create final output structure
    output = {
        'nodes': nodes_info,
        'edges': all_dependencies,
        'summary': {
            'total_nodes': len(nodes_info),
            'total_edges': len(all_dependencies),
            'include_edges': len(include_deps),
            'dependency_edges': len(dependency_deps)
        }
    }
    
    # Write to JSON file
    output_file = os.path.join(output_dir, 'dependency_graph.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nDependency graph written to: {output_file}")
    print(f"\nSummary:")
    print(f"  - Total nodes: {len(nodes_info)}")
    print(f"  - Total edges: {len(all_dependencies)}")
    print(f"  - Include edges: {len(include_deps)}")
    print(f"  - Dependency edges: {len(dependency_deps)}")
    
    # Also write a simplified version for graph visualization tools
    simplified_edges = []
    for edge in all_dependencies:
        simplified_edges.append({
            'data': {
                'source': edge['source'],
                'target': edge['target'],
                'label': edge['label'],
                'type': edge['type']
            }
        })
    
    simplified_output = {
        'elements': {
            'nodes': [{'data': {'id': name, 'label': name, **info}} for name, info in nodes_info.items()],
            'edges': simplified_edges
        }
    }
    
    simplified_file = os.path.join(output_dir, 'dependency_graph_cytoscape.json')
    with open(simplified_file, 'w', encoding='utf-8') as f:
        json.dump(simplified_output, f, indent=2, ensure_ascii=False)
    
    print(f"\nCytoscape format written to: {simplified_file}")

if __name__ == '__main__':
    main()
