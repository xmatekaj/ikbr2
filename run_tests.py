# run_tests.py 
import unittest
import sys
import os

#  project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

def run_tests():
    """Run all tests in the tests directory"""
    # Add project root to Python path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    # Discover and run tests
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests', pattern='test_*.py')
    
    # Run tests with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Return exit code based on test success
    return 0 if result.wasSuccessful() else 1

if __name__ == '__main__':
    sys.exit(run_tests())