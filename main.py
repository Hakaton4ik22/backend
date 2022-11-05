from fastapi import FastAPI
from app.handlers import router
from fastapi.middleware.cors import CORSMiddleware


def get_application() -> FastAPI:
	application = FastAPI()
	application.include_router(router)

	application.add_middleware(
                                CORSMiddleware,
                                allow_origins=["*"],
                                allow_credentials=True,
                                allow_methods=["*"],
                                allow_headers=["*"],
                                )
	return application


app = get_application()