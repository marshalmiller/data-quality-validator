#!/usr/bin/env python3
"""
Installation verification script for DataQualityValidator
Tests that all required dependencies are properly installed.
"""

def test_installation():
    """Test that all dependencies are working correctly."""
    print("🔍 Testing DataQualityValidator Installation...")
    print("=" * 60)
    
    # Test 1: Core imports
    try:
        import pandas as pd
        import numpy as np
        import re
        from datetime import datetime
        from pathlib import Path
        print("✅ Core Python libraries: OK")
    except ImportError as e:
        print(f"❌ Core libraries error: {e}")
        return False
    
    # Test 2: Excel support
    try:
        import openpyxl
        print("✅ Excel support (openpyxl): OK")
    except ImportError as e:
        print(f"❌ Excel support error: {e}")
        print("   Install with: pip install openpyxl")
        return False
    
    # Test 3: PDF generation
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate
        print("✅ PDF generation (reportlab): OK")
    except ImportError as e:
        print(f"❌ PDF generation error: {e}")
        print("   Install with: pip install reportlab")
        return False
    
    # Test 4: Visualization
    try:
        import matplotlib.pyplot as plt
        import matplotlib.patches as patches
        print("✅ Visualization (matplotlib): OK")
    except ImportError as e:
        print(f"❌ Visualization error: {e}")
        print("   Install with: pip install matplotlib")
        return False
    
    # Test 5: DataQualityValidator itself
    try:
        from DataQualityValidator import DataQualityValidator
        print("✅ DataQualityValidator import: OK")
    except ImportError as e:
        print(f"❌ DataQualityValidator error: {e}")
        return False
    
    # Test 6: Type stubs (optional but recommended)
    type_stub_status = []
    try:
        import pandas_stubs
        type_stub_status.append("pandas-stubs")
    except ImportError:
        pass
    
    try:
        import types_reportlab
        type_stub_status.append("types-reportlab")
    except ImportError:
        pass
        
    try:
        import types_openpyxl
        type_stub_status.append("types-openpyxl")
    except ImportError:
        pass
    
    if type_stub_status:
        print(f"✅ Type stubs available: {', '.join(type_stub_status)}")
        print("   (Better IDE autocomplete and type checking)")
    else:
        print("⚠️  Type stubs not installed (optional)")
        print("   Install with: pip install pandas-stubs types-reportlab types-openpyxl")
    
    # Test 7: Quick functionality test
    try:
        test_df = pd.DataFrame({'test': [1, 2, 3]})
        validator = DataQualityValidator('dummy1.xlsx', 'dummy2.xlsx')
        print("✅ Basic functionality test: OK")
    except Exception as e:
        print(f"❌ Functionality test error: {e}")
        return False
    
    print("=" * 60)
    print("🎉 ALL TESTS PASSED!")
    print("✅ DataQualityValidator is ready to use")
    print("\n📚 Next steps:")
    print("1. Prepare your Excel data files")
    print("2. Run: python3 -c 'from DataQualityValidator import DataQualityValidator'")
    print("3. Create a validator instance and run validation")
    
    return True

if __name__ == "__main__":
    success = test_installation()
    if not success:
        print("\n❌ Installation issues detected.")
        print("💡 Try: pip install -r requirements.txt")
        exit(1)
    else:
        print("\n🚀 Installation verified successfully!")
        exit(0)