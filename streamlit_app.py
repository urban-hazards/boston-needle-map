"""Streamlit Cloud entry point — runs the main app module."""

import runpy

runpy.run_module("boston_needle_map.app", run_name="__main__")
