#!/usr/bin/env python3
"""
XML Validation Script
Validates latest.xml against schema.xsd
"""

from lxml import etree
import os
import sys

def validate_xml_against_schema(xml_file_path, xsd_file_path):
    """
    Validates XML file against XSD schema and returns validation results.
    """
    try:
        # Parse XSD schema
        schema_doc = etree.parse(xsd_file_path)
        schema = etree.XMLSchema(schema_doc)
        
        # Parse XML file
        xml_doc = etree.parse(xml_file_path)
        
        # Validate XML against schema
        is_valid = schema.validate(xml_doc)
        
        validation_results = {
            "is_valid": is_valid,
            "errors": [],
            "warnings": []
        }
        
        if not is_valid:
            # Get detailed error information
            for error in schema.error_log:
                validation_results["errors"].append({
                    "line": error.line,
                    "column": error.column,
                    "message": error.message,
                    "domain": error.domain_name,
                    "type": error.type_name
                })
        
        return validation_results
        
    except Exception as e:
        return {
            "is_valid": False,
            "errors": [{"message": f"Validation failed: {str(e)}"}],
            "warnings": []
        }

def main():
    xml_path = 'xml_files/latest.xml'
    xsd_path = 'xml_files/schema.xsd'
    
    print("🔍 XML Validation Tool")
    print("=" * 50)
    
    if not os.path.exists(xml_path):
        print(f"❌ XML file not found: {xml_path}")
        return
        
    if not os.path.exists(xsd_path):
        print(f"❌ XSD schema file not found: {xsd_path}")
        return
    
    print(f"📄 XML file: {xml_path}")
    print(f"📋 Schema file: {xsd_path}")
    print("\n🔄 Starting validation...")
    
    results = validate_xml_against_schema(xml_path, xsd_path)
    
    print(f"\n📊 Validation Results:")
    print(f"   Valid: {'✅ YES' if results['is_valid'] else '❌ NO'}")
    print(f"   Errors: {len(results['errors'])}")
    print(f"   Warnings: {len(results['warnings'])}")
    
    if results['errors']:
        print(f"\n🚨 VALIDATION ERRORS:")
        print("=" * 50)
        for i, error in enumerate(results['errors'][:20], 1):  # Show first 20 errors
            line_info = f"Line {error.get('line', '?')}" if error.get('line') else "Unknown line"
            print(f"{i:2d}. {line_info}: {error['message']}")
            
        if len(results['errors']) > 20:
            print(f"    ... and {len(results['errors']) - 20} more errors")
            
        print(f"\n💡 Total errors found: {len(results['errors'])}")
    else:
        print("\n🎉 SUCCESS! No validation errors found!")
        print("✅ XML file is valid according to the XSD schema")

if __name__ == "__main__":
    main()
