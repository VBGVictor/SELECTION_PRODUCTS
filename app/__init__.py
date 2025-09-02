from flask import Flask

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'uma-chave-secreta-muito-segura'  

    from . import routes
    app.register_blueprint(routes.main_bp)

    return app
