# 이 파일이 있어야 fastapi-cli가 여기서 한 단계 더 올라가 프로젝트 루트를
# sys.path에 넣습니다. 그래야 각 앱에서 루트의 common.py를 import할 수 있습니다.
# (fastapi_cli/discover.py 는 __init__.py 가 있는 동안만 부모로 거슬러 올라갑니다)
