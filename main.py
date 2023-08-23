from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from tkinter import Tk
import tkinter.filedialog as fd
import json

DATABASE_NAME = 'file.sqlite'
engine = create_engine(f'sqlite:///{DATABASE_NAME}')
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# DATABASE_NAME = 'mydatabase'
# DATABASE_USER = 'myuser'
# DATABASE_PASSWORD = 'mypassword'
# DATABASE_HOST = 'localhost'
# DATABASE_PORT = '5432'
#
# engine = create_engine(
#     f'postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}'
# )
# Session = sessionmaker(bind=engine)
# session = Session()

class Question(Base):
    __tablename__ = 'question'
    id = Column(Integer, primary_key=True)
    question = Column(String)
    bestAnswer = Column(String)
    alternative_questions = relationship('AlternativeQuestion', backref='parent_question')

    def __init__(self, question, bestAnswer):
        self.question = question
        self.bestAnswer = bestAnswer

    def __repr__(self):
        info = f'Вопросы [номер: {self.id}, вопрос: {self.question}, ответ: {self.bestAnswer}]'
        return info


class AlternativeQuestion(Base):
    __tablename__ = 'alternativeQuestion'
    id = Column(Integer, primary_key=True)
    question = Column(String)
    questionId = Column(Integer, ForeignKey('question.id'))

    def __init__(self, question, questionId):
        self.question = question
        self.questionId = questionId

    def __repr__(self):
        return f'Альтернативный Вопрос [ID: {self.id}, вопрос: {self.question}, ID вопроса: {self.questionId}]'


def create_db():
    Base.metadata.create_all(engine)


def create_database(load_fake_data: bool = True):
    create_db()

    if load_fake_data:
        root = Tk()
        root.withdraw()
        file_path = fd.askopenfilename()

        if file_path:
            models = read_jsonl(file_path)
            add_to_database(models, session)
            move_duplicates_to_alternative(session)

        root.destroy()


class PromptCompletion:
    def __init__(self, prompt, completion):
        self.prompt = prompt
        self.completion = completion


def read_jsonl(file_path):
    data = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                json_data = json.loads(line)
                model = PromptCompletion(json_data['prompt'], json_data['completion'])
                data.append(model)

    return data


def add_to_database(models, session):
    existing_answers = set()
    questions_dict = {}  # Словарь для отслеживания уникальных вопросов и их соответствующих ID

    for model in models:
        question = session.query(Question).filter(Question.bestAnswer == model.completion).first()

        if question:
            existing_answers.add(model.completion)
            if model.completion not in questions_dict:
                questions_dict[model.completion] = question.id
            new_alternative = AlternativeQuestion(question=model.prompt, questionId=question.id)
            session.add(new_alternative)
        else:
            new_question = Question(question=model.prompt, bestAnswer=model.completion)
            session.add(new_question)
            existing_answers.add(model.completion)
            questions_dict[model.completion] = new_question.id

    # Добавление вопросов с новыми ответами
    new_answers = set(model.completion for model in models) - existing_answers

    for model in models:
        if model.completion in new_answers:
            question = session.query(Question).filter(Question.bestAnswer == model.completion).first()

            if question:
                questions_dict[model.completion] = question.id
                new_alternative = AlternativeQuestion(question=model.prompt, questionId=question.id)
                session.add(new_alternative)
            else:
                new_question = Question(question=model.prompt, bestAnswer=model.completion)
                session.add(new_question)
                questions_dict[model.completion] = new_question.id

    session.commit()
    print("Data added successfully!")


def move_duplicates_to_alternative(session):
    # Получаем дублирующиеся ответы
    duplicates = (
        session.query(Question.bestAnswer)
        .group_by(Question.bestAnswer)
        .having(func.count() > 1)
        .all()
    )
    duplicate_answers = [d[0] for d in duplicates]

    # Выбираем вопросы с дублирующимися ответами
    questions_to_move = (
        session.query(Question)
        .filter(Question.bestAnswer.in_(duplicate_answers))
        .all()
    )

    for question in questions_to_move:
        # Создаем альтернативный вопрос
        alternative_question = AlternativeQuestion(
            question=question.question, questionId=question.id
        )
        session.add(alternative_question)

    # Удаляем вопросы с дублирующимися ответами из таблицы question
    (
        session.query(Question)
        .filter(Question.bestAnswer.in_(duplicate_answers))
        .delete(synchronize_session=False)
    )

    session.commit()
    print("Duplicates moved to alternativeQuestion!")


create_database()
