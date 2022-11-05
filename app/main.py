from fastapi import FastAPI
# from app.config import API_PREFIX, DEBUG, PROJECT_NAME, VERSION
from app.handlers import router
from fastapi.middleware.cors import CORSMiddleware

#origins = ["*"]
            # "http://localhost",
            # "http://localhost:80"]#,
            # "http://localhost:8080",
            # "http://localhost:4200",
            # "http://95.104.193.81",
            # "http://95.104.193.81:80",
            # "http://95.104.193.81:8080",
            # "http://95.104.193.81:4200",
            # "http://95.104.198.226",
            # "http://95.104.198.226:80",
            # "http://95.104.198.226:8080",
            # "http://95.104.198.226:4200",
            # ]




def get_application() -> FastAPI:
    application = FastAPI()#title=PROJECT_NAME, debug=DEBUG, version=VERSION)
    application.include_router(router)#, prefix=API_PREFIX)

    application.add_middleware(
                                CORSMiddleware,
                                allow_origins=["*"],
                                allow_credentials=True,
                                allow_methods=["*"],
                                allow_headers=["*"],
                                )

    return application


app = get_application()

